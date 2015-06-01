###
	# Log.io Log Server #

	Relays inbound log messages to web clients

	LogServer receives log messages via TCP:
	"+log|my_stream|my_server_host|info|this is a log message\r\n"

	Announce a node, optionally with stream associations
	"+node|my_server_host\r\n"
	"+node|my_server_host|my_stream1,my_stream2,my_stream3\r\n"

	Announce a stream, optionally with node associations
	"+stream|my_stream1\r\n"
	"+stream|my_stream1|my_server_host1,my_host_server2\r\n"

	Remove a node or stream
	"-node|my_server_host1\r\n"
	"-stream|stream2\r\n"

	WebServer listens for events emitted by LogServer and
	forwards them to web clients via socket.io

	# Usage:
	logServer = new LogServer port: 28777
	webServer = new WebServer logServer, port: 28778
	webServer.run()
###

events = require 'events'
fs = require 'fs'
http = require 'http'
https = require 'https'
net = require 'net'

express = require 'express'
io = require 'socket.io'
winston = require 'winston'

class _LogObject
	_type: 'object'
	_pclass: ->
	_pcollection: ->
	constructor: (@logServer, @name, _pairs=[]) ->
		@logServer.emit "add_#{@_type}", this

		@pairs = {}

		@pclass = @_pclass()
		@pcollection = @_pcollection()

		@addPair pname for pname in _pairs

	addPair: (pname) ->
		unless pair = @pairs[pname]
			unless pair = @pcollection[pname]
				pair = @pcollection[pname] = new @pclass @logServer, pname

			pair.pairs[@name] = this
			@pairs[pname] = pair

			@logServer.emit "add_#{@_type}_pair", this, pname

	remove: ->
		@logServer.emit "remove_#{@_type}", this
		delete p.pairs[@name] for name, p of @pairs

	toDict: ->
		name: @name
		pairs: (name for name, obj of @pairs)

class LogNode extends _LogObject
	_type: 'node'
	_pclass: -> LogStream
	_pcollection: -> @logServer.streams

class LogStream extends _LogObject
	_type: 'stream'
	_pclass: -> LogNode
	_pcollection: -> @logServer.nodes

###
	LogServer listens for TCP connections.  It parses & validates
	inbound TCP messages, and emits events.
###

class LogServer extends events.EventEmitter
	constructor: (config={}) ->
		{@host, @port} = config

		# Per default, use localhost:28777
		@host = '127.0.0.1' unless @host?
		@port = 28777       unless @port?

		@debug = config.logging ? winston
		@delimiter = config.delimiter ? '\r\n'

		@nodes = {}
		@streams = {}

	run: ->
		# Create TCP listener socket
		@server = net.createServer (socket) =>
			socket._buffer = ''

			socket.on 'data', @_receive.bind   this, socket
			socket.on 'error', @_tearDown.bind this, socket
			socket.on 'close', @_tearDown.bind this, socket

		@server.listen @port, @host

	_tearDown: (socket) ->
		# Destroy a client socket
		@debug.error 'Lost TCP connection...'
		if socket.node
			@_removeNode socket.node.name
			delete socket.node

	_receive: (socket, data) =>
		part = data.toString()
		socket._buffer += part
		@debug.debug "Received TCP message: #{part}"
		@_flush socket if socket._buffer.indexOf @delimiter >= 0

	_flush: (socket) =>
		# Handle messages in socket buffer
		# Pause socket while modifying buffer
		socket.pause()
		[msgs..., socket._buffer] = socket._buffer.split @delimiter
		socket.resume()
		@_handle socket, msg for msg in msgs

	_handle: (socket, msg) ->
		@debug.debug "Handling message: #{msg}"
		[mtype, args...] = msg.split '|'
		switch mtype
			when '+log' then @_newLog args...
			when '+node' then @_addNode args...
			when '+stream' then @_addStream args...
			when '-node' then @_removeNode args...
			when '-stream' then @_removeStream args...
			when '+bind' then @_bindNode socket, args...
			else @debug.error "Invalid TCP message: #{msg}"

	_addNode: (nname, snames='') ->
		@addEntity nname, snames, @nodes, LogNode, 'node'

	_addStream: (sname, nnames='') ->
		@addEntity sname, nnames, @streams, LogStream, 'stream'

	_removeNode: (nname) ->
		@__remove nname, @nodes, 'node'

	_removeStream: (sname) ->
		@__remove sname, @streams, 'stream'

	_newLog: (sname, nname, logLevel, message...) ->
		message = message.join '|'
		@debug.debug "Log message: (#{sname}, #{nname}, #{logLevel}) #{message}"
		node = @nodes[nname] or @_addNode nname, sname
		stream = @streams[sname] or @_addStream sname, nname
		@emit 'new_log', stream, node, logLevel, message

	addEntity: (name, pnames, _collection, _objClass, objName) ->
		@debug.info "Adding #{objName}: #{name} (#{pnames})"
		pnames = pnames.split ','
		obj = _collection[name] = _collection[name] or new _objClass @, name, pnames
		obj.addPair p for p in pnames when not obj.pairs[p]

	__remove: (name, _collection, objType) ->
		if obj = _collection[name]
			@debug.info "Removing #{objType}: #{name}"
			obj.remove()
			delete _collection[name]

	_bindNode: (socket, obj, nname) ->
		if node = @nodes[nname]
			@debug.info "Binding node '#{nname}' to TCP socket"
			socket.node = node
			@_ping socket

	_ping: (socket) ->
		if socket.node
			socket.write 'ping'
			setTimeout (=> @_ping socket), 2000

###
	WebServer relays LogServer events to web clients via socket.io.
###

class WebServer
	constructor: (@logServer, config) ->
		{@host, @port, @auth} = config

		{@nodes, @streams} = @logServer

		@restrictSocket = config.restrictSocket ? '*:*'

		@debug = config.logging ? winston

		# Create express server
		app = @_buildServer config

		@http = @_createServer config, app

	_buildServer: (config) ->
		app = express()

		if @auth?
			app.use express.basicAuth @auth.user, @auth.pass

		if config.restrictHTTP
			ips = new RegExp config.restrictHTTP.join '|'

			app.all '/', (req, res, next) =>
				if not req.ip.match ips
					return res.send 403, "Your IP (#{req.ip}) is not allowed."

				next()

		staticPath = config.staticPath ? __dirname + '/../'

		app.use express.static staticPath

	_createServer: (config, app) ->
		unless config.ssl
			http.createServer app
		else
			https.createServer
				key: fs.readFileSync config.ssl.key
				cert: fs.readFileSync config.ssl.cert
			, app

	run: ->
		@debug.info 'Starting Log.io Web Server...'

		@logServer.run()

		io = io.listen @http.listen @port, @host

		io.set 'log level', 1
		io.set 'origins', @restrictSocket

		# sockets on which we propagate to
		# each listener; broadcast
		{sockets} = io

		_on = @logServer.on.bind @logServer
		_emit = (_event, msg) =>
			@debug.debug "Relaying: #{_event}"
			sockets.emit _event, msg

		# Bind events from LogServer to web client
		_on 'add_node', (node) ->
			_emit 'add_node', node.toDict()

		_on 'add_stream', (stream) ->
			_emit 'add_stream', stream.toDict()

		_on 'add_stream_pair', (stream, nname) ->
			_emit 'add_pair',
				stream: stream.name
				node: nname

		_on 'add_node_pair', (node, sname) ->
			_emit 'add_pair',
				stream: sname
				node: node.name

		_on 'remove_node', (node) ->
			_emit 'remove_node', node.toDict()

		_on 'remove_stream', (stream) ->
			_emit 'remove_stream', stream.toDict()

		# Bind new log event from Logserver to web client
		_on 'new_log', (stream, node, level, message) ->
			# announce new node/stream pair
			_emit 'ping',
				stream: stream.name
				node: node.name

			# Only propagate new messages to clients
			# which listen to the respective node/stream
			# pair. ; TODO: Listen by default?
			sockets
			.in("#{stream.name}:#{node.name}")
			.emit 'new_log',
				stream: stream.name
				node: node.name
				level: level
				message: message

		{logNodes, logStreams} = this

		# Bind web client connection, events to web server
		sockets.on 'connection', (wclient) ->
			wclient.on 'watch',   wclient.join.bind wclient
			wclient.on 'unwatch', wclient.leave.bind wclient

			wclient.emit 'add_node', node.toDict() for n, node of logNodes
			wclient.emit 'add_stream', stream.toDict() for s, stream of logStreams

			for n, node of logNodes
				for s, stream of node.pairs
					wclient.emit 'add_pair', {stream: s, node: n}

			wclient.emit 'initialized'

		@debug.info 'Server started, listening...'

exports.LogServer = LogServer
exports.WebServer = WebServer
