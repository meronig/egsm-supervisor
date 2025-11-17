var fs = require('fs');
var LOG = require('./modules/egsm-common/auxiliary/logManager')
var DBCONFIG = require('./modules/egsm-common/database/databaseconfig');
var CONNCONFIG = require('./modules/egsm-common/config/connectionconfig');

module.id = 'MAIN'
const CONFIG_FILE = './config/config.xml'

var filecontent = fs.readFileSync(CONFIG_FILE, 'utf8')

CONNCONFIG.applyConfig(filecontent)

DBCONFIG.initDatabaseConnection(CONNCONFIG.getConfig().database_host, CONNCONFIG.getConfig().database_port, CONNCONFIG.getConfig().database_region,
    CONNCONFIG.getConfig().database_access_key_id, CONNCONFIG.getConfig().database_secret_access_key)

DBCONFIG.initTables().then(()=>{
    LOG.logSystem('DEBUG', 'Tables have been initialized', module.id)
})