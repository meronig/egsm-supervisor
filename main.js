var fs = require('fs');
var xml2js = require('xml2js');

var LOG = require('./modules/egsm-common/auxiliary/logManager')
var AUTOCONFIG = require('./modules/config/autoconfig')
var MQTTCOMM = require('./modules/communication/mqttcommunication')
var SOCKET = require('./modules/communication/socketserver')
var LIBRARY = require('./modules/resourcemanager/processlibrary')
var DBCONFIG = require('./modules/egsm-common/database/databaseconfig');
var CONNCONFIG = require('./modules/egsm-common/config/connectionconfig');
const { Broker } = require('./modules/egsm-common/auxiliary/primitives');

const CONFIG_FILE = './config/config.xml'
module.id = "MAIN"

async function startSupervisor() {
    LOG.logSystem('DEBUG', 'Application started...', module.id)

    var filecontent = fs.readFileSync(CONFIG_FILE, 'utf8')

    CONNCONFIG.applyConfig(filecontent)

    DBCONFIG.initDatabaseConnection(CONNCONFIG.getConfig().database_host, CONNCONFIG.getConfig().database_port, CONNCONFIG.getConfig().database_region,
        CONNCONFIG.getConfig().database_access_key_id, CONNCONFIG.getConfig().database_secret_access_key)

    await MQTTCOMM.initBrokerConnection(CONNCONFIG.getConfig().primary_broker)

    LIBRARY.exportProcessLibraryToDatabase()

    process.on('SIGINT', () => {
        LOG.logSystem('DEBUG', 'SIGINT signal caught. Shutting down supervisor...', module.id)
        process.exit()
    });
}

startSupervisor().catch(error => {
    LOG.logSystem('ERROR', `Failed to start supervisor: ${error}`, module.id)
    process.exit(1)
})
