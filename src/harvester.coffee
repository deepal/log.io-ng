###
	# Log.io Log Harvester #

	Watches local files and sends new log message to server via TCP.

	# Sample configuration:
	config =
		nodeName: 'my_server01'
		logStreams:
			web_server: [
				'/var/log/nginx/access.log',
				'/var/log/nginx/error.log'
			],
		server:
			host: '0.0.0.0',
			port: 28777

	# Sends the following TCP messages to the server:
	"+node|my_server01|web_server\r\n"
	"+bind|node|my_server01\r\n"
	"+log|web_server|my_server01|info|this is log messages\r\n"

	# Usage:
	harvester = new LogHarvester config
	harvester.run()

###

fs = require 'fs'
net = require 'net'

events = require 'events'
winston = require 'winston'

###
	LogStream is a group of local files paths.  It watches each file for
	changes, extracts new log messages, and emits 'new_log' events.
###

class LogStream extends events.EventEmitter
	constructor: (@name, @paths, @debug) ->

	watch: ->
		@debug.info "Starting log stream: '#{@name}'"
		@_watchFile path for path in @paths

		this

	_watchFile: (path) ->
			if not fs.existsSync path
				@debug.error "File doesn't exist: '#{path}'"
				setTimeout (=> @_watchFile path), 1000
				return

			@debug.info "Watching file: '#{path}'"

			currSize = fs.statSync(path).size

			watcher = fs.watch path, (event, filename) =>
				if event is 'rename'
					# File has been rotated, start new watcher
					watcher.close()

					@_watchFile path

				if event is 'change'
					# Capture file offset information for change event
					fs.stat path, (err, stat) =>
						@_readNewLogs path, stat.size, currSize

						currSize = stat.size

	_readNewLogs: (path, curr, prev) ->
		# Use file offset information to stream new log lines from file
		return if curr < prev
		rstream = fs.createReadStream path,
			encoding: 'utf8'
			start: prev
			end: curr
		# Emit 'new_log' event for every captured log line
		rstream.on 'data', (data) =>
			lines = data.split "\n"
			@emit 'new_log', line for line in lines when line

###
	LogHarvester creates LogStreams and opens a persistent TCP connection to the server.

	On startup it announces itself as Node with Stream associations.
	Log messages are sent to the server via string-delimited TCP messages

	- NOTE -
	Support for 'nodeName' and 'logStreams' is deprecated in
	the process of simplifying the API alltogether. They will
	be replaced by respectively 'name' and 'streams'.

	LogHarvester
		name: "appserver"
		streams: ['errors', 'registrations']
###

class LogHarvester
	constructor: (config) ->
		{@name, @server} = config

		if not @name? and config.nodeName?
			@name = config.nodeName

		# Per default, use localhost:28777
		unless @server?
			@server =
				host: '127.0.0.1'
				port: 28777

		# queue for events in case the harvester
		# or endpoint dies and the socket is closed.
		@queue = []

		@delimiter = config.delimiter ? '\r\n'
		@debug = config.logging ? winston

		@streams =
			for stream, paths of (config.streams ? (config.logStreams ? []))
				new LogStream stream, paths, @debug

	run: ->
		@_connect (err) =>
			@_announce()

		@streams.forEach (stream) =>
			stream.watch().on 'new_log', (msg) =>
				return unless @instance

				@debug.debug "Sending log: (#{stream.name}) #{msg}"
				@send '+log', stream.name, @name, 'info', msg

	_announce: ->
		streamList = (l.name for l in @streams).join ","

		@debug.info "Announcing: #{@name} (#{streamList})"

		@send '+node', @name, streamList
		@send '+bind', 'node', @name

	send: (type, args...) =>
		msg = "#{type}|#{args.join '|'}#{@delimiter}"
		msg = do msg.trim

		@_connect (err) =>
			@queue.push msg

			do @_commit

	###
		Below is based on https://gist.github.com/KenanSulayman/f7e55a28df614c520576.
	###

	_commit: () =>
		return unless @instance

		# cutcopy the queue
		_queue = (do @queue.shift for item in @queue)

		@instance.write _queue.join '' if _queue.length

	_connect: (cb) =>
		if @instance?
			if @instance.readyState is 'open'
				return cb null
			else
				return cb true

		@instance = net.connect
			host: @server.host
			port: @server.port

		@instance.setKeepAlive true
		@instance.setNoDelay()

		@instance
		.on 'connect', =>
			do @_commit

			@queue = []
			@retries = 0
			@connected = true

			cb null

		.on 'error', (e) ->
			console.log e

		.on 'end', ( -> )

		.on 'close', =>
			@_reconnect cb

		.on 'timeout', (e) =>
			if @instance.readyState isnt 'open'
				do @instance.destroy

				@_reconnect cb

	_reconnect: (cb) =>
		interval = Math.pow 2, @retries

		@connected = false

		setTimeout =>
			@retries += 1

			# reestablish
			@_connect cb
		, interval * 1000

exports.LogHarvester = LogHarvester
