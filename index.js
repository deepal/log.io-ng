module.exports = {
    LogHarvester: require(__dirname + '/lib/harvester.js').LogHarvester,
    LogServer: require(__dirname + '/lib/server.js').LogServer,
    WebServer: require(__dirname + '/lib/server.js').WebServer
}