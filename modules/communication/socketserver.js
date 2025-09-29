/**
 * Interface for the frontend application
 * The module starts a websocket server the frontend application can connect to, all supervisor-front-end communication is happening through this
 */
var UUID = require("uuid");
var WebSocketServer = require('websocket').server;
var http = require('http');
const schedule = require('node-schedule');

var LOG = require('../egsm-common/auxiliary/logManager');
var PROCESSLIB = require('../resourcemanager/processlibrary');
var MQTTCOMM = require('./mqttcommunication')
var DDB = require('../egsm-common/database/databaseconnector');

module.id = 'SOCKET'

const SOCKET_PORT = 8081
const OVERVIEW_UPDATE_PERIOD = 5 //Update period in secs of Overview and System Information frontend modules

//Front-end module keys
const MODULE_SYSTEM_INFORMATION = 'system_information'
const MODULE_OVERVIEW = 'overview'
const MODULE_WORKER_DETAIL = 'worker_detail'
const MODULE_ENGINES = 'process_operation'
const MODULE_PROCESS_LIBRARY = 'process_library'
const MODULE_NEW_PROCESS_INSTANCE = 'new_process_instance'
const MODULE_ARTIFACTS = 'artifact_detail'
const MODULE_STAKEHOLDERS = 'stakeholder_detail'
const MODULE_NOTIFICATIONS = 'notifications'
const MODULE_NEW_PROCESS_GROUP = 'new_process_group'
const MODULE_AGGREGATORS = 'aggregators'
const MODULE_PROCESS_TYPE_DETAIL = 'process_type_detail'

var sessions = new Map() //session_id -> session related data

var server = http.createServer(function (request, response) {
    LOG.logSystem('DEBUG', 'Received request', module.id)
    response.writeHead(404);
    response.end();
});
server.listen(SOCKET_PORT, function () {
    LOG.logSystem('DEBUG', `Socket Server is listening on port ${SOCKET_PORT}`, module.id)
});

wsServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: false
});

function originIsAllowed(origin) {
    return true;
}

wsServer.on('request', function (request) {
    if (!originIsAllowed(request.origin)) {
        request.reject();
        LOG.logSystem('DEBUG', `Connection from origin ${request.origin} rejected`, module.id)
        return;
    }

    var connection = request.accept('data-connection', request.origin);
    LOG.logSystem('DEBUG', `Connection from origin ${request.origin} accepted`, module.id)
    //Object to store session data
    var sessionId = UUID.v4()
    sessions.set(sessionId, {
        connection: connection,
        subscriptions: new Set()
    })

    //Update System Information and Overview module periodically
    const periodicUpdaterJob = schedule.scheduleJob(` */${OVERVIEW_UPDATE_PERIOD} * * * * *`, function () {
        LOG.logSystem('DEBUG', `Sending update to Overview and System Information module`, module.id)
        getSystemInformationModuleUpdate().then((data) => {
            connection.sendUTF(JSON.stringify(data))
        })
        getOverviewModuleUpdate().then((data) => {
            connection.sendUTF(JSON.stringify(data))
        })
    });

    connection.on('message', function (message) {
        LOG.logSystem('DEBUG', `Message received`, module.id)
        messageHandler(message.utf8Data, sessionId).then((data) => {
            connection.sendUTF(JSON.stringify(data))
        })

    });

    connection.on('close', function (reasonCode, description) {
        periodicUpdaterJob.cancel()
        //Cancel session related subscriptions
        sessions.get(sessionId).subscriptions.forEach(subscription => {
            MQTTCOMM.unsubscribeNotificationTopic('notification/' + subscription)
        });
        sessions.delete(sessionId)

        LOG.logSystem('DEBUG', `Peer ${connection.remoteAddress} disconnected`, module.id)
    });
});

//Subscribe to notifications from MqttCommunication module
//Feature used in front-end->Notification menu
MQTTCOMM.EVENT_EMITTER.on('notification', (topic, notification) => {
    for (let [key, value] of sessions) {
        value.subscriptions.forEach(subscription => {
            if (subscription == topic.replace('notification/', '')) {
                var message = {
                    module: MODULE_NOTIFICATIONS,
                    payload: {
                        type: "new_notification",
                        notification: notification
                    }
                }
                value.connection.sendUTF(JSON.stringify(message))
            }
        });
    }
});

/**
 * Main messagehandler function
 * Calls the necessary functions to execute the requests and/or process the received messages
 * @param {Object} message Message object from frontend 
 * @param {String} sessionid Id of the session the message received from 
 * @returns A promise to the response message
 */
async function messageHandler(message, sessionid) {
    var msgObj = JSON.parse(JSON.parse(message))

    if (msgObj['type'] == 'update_request') {
        switch (msgObj['module']) {
            case MODULE_OVERVIEW:
                return getOverviewModuleUpdate()
            case MODULE_SYSTEM_INFORMATION:
                return getSystemInformationModuleUpdate()
            case MODULE_WORKER_DETAIL:
                return getWorkerEngineList(msgObj['payload']['worker_name'])
            case MODULE_ENGINES:
                return getProcessEngineList(msgObj['payload']['process_instance_id'])
            case MODULE_PROCESS_LIBRARY:
                if (msgObj['payload']['type'] == 'process_type_list') {
                    return getProcessTypeList()
                }
                else if (msgObj['payload']['type'] == 'get_process_group') {
                    return readProcessGroup(msgObj['payload']['process_goup_id'])
                }
            case MODULE_ARTIFACTS:
                if (msgObj['payload']['type'] == 'search') {
                    return getArtifact(msgObj['payload']['artifact_type'], msgObj['payload']['artifact_id'])
                }
            case MODULE_STAKEHOLDERS:
                return getStakeholder(msgObj['payload']['stakeholder_name'])
            case MODULE_NOTIFICATIONS:
                if (msgObj['payload']['type'] == 'get_stakeholder_list') {
                    return getStakeholderList()
                }
                else if (msgObj['payload']['type'] == 'get_past_notifications') {
                    return getPastNotifications(msgObj['payload']['stakeholders'])
                }
                else if (msgObj['payload']['type'] == 'subscribe_notifications') {
                    return setNotificationSubscriptions(msgObj['payload']['stakeholders'], sessionid)
                }
            case MODULE_PROCESS_TYPE_DETAIL:
                if (msgObj['payload']['type'] == 'get_process_type_aggregation') {
                    return getProcessTypeAggregation(msgObj['payload']['process_type'])
                }
            case MODULE_AGGREGATORS:
                if (msgObj['payload']['request_type'] == 'available_aggregations') {
                    return getAvailableAggregations()
                }
                else if (msgObj['payload']['request_type'] == 'complete_aggregation_data') {
                    return getCompleteAggregationData(msgObj['payload']['process_type'])
                }
                break;
        }
    }
    else if (msgObj['type'] == 'command') {
        switch (msgObj['module']) {
            case MODULE_NEW_PROCESS_INSTANCE:
                return await createProcessInstance(msgObj['payload']['process_type'], msgObj['payload']['instance_name'], msgObj['payload']['bpmn_job_start'])
            case MODULE_ENGINES:
                return deleteProcessInstance(msgObj['payload']['process_type'], msgObj['payload']['process_instance_id'])
            case MODULE_ARTIFACTS:
                if (msgObj['payload']['type'] == 'create') {
                    return createNewArtifact(msgObj['payload']['artifact'])
                }
                else if (msgObj['payload']['type'] == 'delete') {
                    //TODO: Add Artifact->delete functionality here!
                }
            case MODULE_STAKEHOLDERS:
                if (msgObj['payload']['type'] == 'create') {
                    return createNewStakeholder(msgObj['payload']['stakeholder_name'])
                }
            case MODULE_NEW_PROCESS_GROUP:
                if (msgObj['payload']['type'] == 'create') {
                    return createNewProcessGroup(msgObj['payload']['group_id'], msgObj['payload']['rules'])
                }
            case MODULE_AGGREGATORS:
                if (msgObj['payload']['type'] == 'create') {
                    return createJobInstance(msgObj['payload']['job_id'], msgObj['payload']['job_definition'])
                }
        }
    }
}

/**
 * Handles update requests from MODULE_SYSTEM_INFORMATION
 * @returns Promise containing the response message
 */
async function getSystemInformationModuleUpdate() {
    var promise = new Promise(async function (resolve, reject) {
        var workers = await MQTTCOMM.getWorkerList()
        var aggregators = await MQTTCOMM.getAggregatorList()
        await Promise.all([workers, aggregators])

        var response = {
            module: MODULE_SYSTEM_INFORMATION,
            payload: {
                system_up_time: process.uptime(),
                worker_number: workers.length,
                aggregator_number: aggregators.length,
            }
        }
        resolve(response)
    });
    return promise
}

/**
 * Handles update requests from MODULE_OVERVIEW
 * @returns Promise containing the response message
 */
async function getOverviewModuleUpdate() {
    var promise = new Promise(async function (resolve, reject) {
        var workers = await MQTTCOMM.getWorkerList()
        var aggregators = await MQTTCOMM.getAggregatorList()
        await Promise.all([workers, aggregators])

        var response = {
            module: MODULE_OVERVIEW,
            payload: {
                workers: workers,
                aggregators: aggregators,
            }
        }
        resolve(response)
    });
    return promise
}

/**
 * Returns the array of engines deployed on the Worker specified as argument
 * @param {String} workername Worker Id to specify the Worker
 * @returns Promise will contain array of Engines (empty array in case of no engine)
 */
async function getWorkerEngineList(workername) {
    var promise = new Promise(async function (resolve, reject) {
        var engines = await MQTTCOMM.getWorkerEngineList(workername)
        await Promise.all([engines])
        var response = {
            module: MODULE_WORKER_DETAIL,
            payload: {
                engines: engines,
            }
        }
        resolve(response)
    });
    return promise
}

/**
 * Returns an array of engines belonging to the process specified by 'process_instance_id' argument
 * @param {String} process_instance_id Process instance ID
 * @returns  Promise will contain an array of Engines (empty array in case of no engine (no process) found)
 */
async function getProcessEngineList(process_instance_id) {
    var promise = new Promise(async function (resolve, reject) {
        var engines = await MQTTCOMM.searchForProcess(process_instance_id)
        await Promise.all([engines])

        if (engines.length > 0) {
            var bpmn_job = await MQTTCOMM.searchForJob(engines[0].type + '/' + process_instance_id + '/bpmn_job')
        }
        var response = {
            module: MODULE_ENGINES,
            payload: {
                engines: engines,
                bpmn_job: bpmn_job?.['payload']?.['job'] || 'not_found'
            }
        }
        resolve(response)
    });
    return promise
}

/**
 * Deletes all engines belonging to the specified process instance specified by 'process_instance_id' argument
 * As a side effect the function will publish the Destruction Process Lifecycle event as well
 * @param {string} process_instance_id Process instance ID
 * @returns Promise to the result of the operation
 */
async function deleteProcessInstance(process_type, process_instance_id) {
    var promise = new Promise(async function (resolve, reject) {
        MQTTCOMM.deleteProcess(process_instance_id).then((result) => {
            var response = {
                module: MODULE_ENGINES,
                payload: {
                    delete_result: result,
                }
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Get list of available Process Type definitions
 * @returns Promise to the response body containing the process types as well
 */
function getProcessTypeList() {
    var promise = new Promise(async function (resolve, reject) {
        DDB.readAllProcessTypes().then((processtypelist) => {
            var response = {
                module: MODULE_PROCESS_LIBRARY,
                payload: {
                    type: 'process_type_list',
                    process_types: processtypelist
                }
            }
            resolve(response)
        })
    });
    return promise
}
//TODO: Add process type level statistics retrieval function (extension of getProcessTypeList())

/**
 * Retrieve information about the specified Process Group
 * @param {String} processgroupname Name of the Process Group 
 * @returns Promise will contain the details of the specified Process Group, or 'not_found' as result
 */
function readProcessGroup(processgroupname) {
    var promise = new Promise(async function (resolve, reject) {
        DDB.readProcessGroup(processgroupname).then((processgroup) => {
            var response = {
                module: MODULE_PROCESS_LIBRARY,
                payload: {
                    type: 'get_process_group',
                    result: "not_found"
                }
            }
            if (processgroup) {
                response.payload.result = 'found'
                response.payload.process_group = processgroup
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Retrieve Details about a specified Artifact
 * @param {String} artifact_type Type of the requested Artifact
 * @param {String} artifact_id Instance ID of the requested Artifact
 * @returns Promise will contain details of Artifact (if found), or 'not_found' as result
 */
function getArtifact(artifact_type, artifact_id) {
    var promise = new Promise(async function (resolve, reject) {
        DDB.readArtifactDefinition(artifact_type, artifact_id).then((artifact) => {
            var response = {
                module: MODULE_ARTIFACTS,
                payload: {
                    type: 'search',
                    result: "not_found"
                }
            }
            if (artifact) {
                response.payload.result = 'found'
                artifact.faulty_rates = JSON.stringify([...artifact.faulty_rates])
                artifact.timing_faulty_rates = JSON.stringify([...artifact.timing_faulty_rates])
                response.payload.artifact = artifact
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Retrieve details about the specified Stakeholder
 * @param {String} stakeholder_name Name of the requested Stakeholder
 * @returns Promise will contain the details of the requested Stakeholder, or 'not_found' as result
 */
function getStakeholder(stakeholder_name) {
    var promise = new Promise(async function (resolve, reject) {
        DDB.readStakeholder(stakeholder_name).then((stakeholder) => {
            var response = {
                module: MODULE_STAKEHOLDERS,
                payload: {
                    type: 'search',
                    result: "not_found"
                }
            }
            if (stakeholder) {
                response.payload.result = 'found'
                response.payload.stakeholder = stakeholder
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Get all defined Stakeholders at once (name and details)
 * @returns Promise will contain a list of stakeholders supplemented with their details stored in the database
 */
function getStakeholderList() {
    var promise = new Promise(async function (resolve, reject) {
        DDB.readAllStakeholder().then((stakeholderList) => {
            var response = {
                module: MODULE_NOTIFICATIONS,
                payload: {
                    type: 'stakeholder_list',
                    stakeholder_list: stakeholderList,
                    result: "ok"
                }
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Retrieve past notifications from Database intended for a specified Stakeholder
 * TODO: Function is not implemented completely! Notifications are currently not logged into database!
 * @param {String} stakeholders Name of the specified Stakeholder 
 * @returns Promise will contain a list of notifications
 */
function getPastNotifications(stakeholders) {
    var promise = new Promise(async function (resolve, reject) {
        var promises = []
        //DDB.read
        DDB.readAllStakeholder().then((notification_list) => {
            var response = {
                module: MODULE_NOTIFICATIONS,
                payload: {
                    type: 'get_past_notifications',
                    notification_list: [],//notification_list,
                    result: "ok"
                }
            }
            resolve(response)
        })
    });
    return promise
}

/**
 * Function subscribes a session to one or more Stakeholder's notifications
 * This function will make the Supervisor to subscribe to the notification topics of the specified stakeholders and
 * it will forward the notifications to the specified session through the established websocket connection
 * A subscription to a certain Stakeholder can be withdraw by calling this function again (with the same session id), 
 * but 'stakeholders' attribute not containing the certain Stakeholder. Each call of the function it will compare the
 * content of 'stakeholders' array, with the current subscription and subscribe and remove to achieve the state specified 
 * by 'stakeholders' array
 * All subscriptions are automatically withdraw in case of socket disconnection
 * without
 * @param {String[]} stakeholders List of Stakeholder names specifying the Stakeholders
 * @param {String} sessionid Session ID 
 * @returns A Promise containing the response body for the Front-end
 */
function setNotificationSubscriptions(stakeholders, sessionid) {
    var promise = new Promise(async function (resolve, reject) {
        var newStakeholders = new Set(stakeholders)
        //unsubscribe from topics which has locally but not in stakeholders
        sessions.get(sessionid).subscriptions.forEach(element => {
            if (!newStakeholders.has(element)) {
                sessions.get(sessionid).subscriptions.delete(element)
                MQTTCOMM.unsubscribeNotificationTopic('notification/' + element)
            }
        });

        stakeholders.forEach(stakeholder => {
            if (!sessions.get(sessionid).subscriptions.has(stakeholder)) {
                sessions.get(sessionid).subscriptions.add(stakeholder)
                MQTTCOMM.subscribeNotificationTopic('notification/' + stakeholder)
            }
        });
        var response = {
            module: MODULE_NOTIFICATIONS,
            payload: {
                type: 'ok',
            }
        }
        resolve(response)
    });
    return promise
}

async function getProcessTypeAggregation(processtype) {
    var promise = new Promise(async function (resolve, reject) {
        var processDb = await DDB.readProcessType(processtype)

        //all bpmn perspective
        //bpmn job cnt
        //instance cnt
        //statistics for each bpmn perspectives

        //look for a real-time aggregation job
        //if the job found we need the real time statistocs for each bpmn perspectives
        var perspectives = []
        var aggregation_job = await MQTTCOMM.searchForJob(processtype + '_real_time_aggregation_job')

        processDb.definition.perspectives.forEach(perspective => {
            perspectives.push({
                name: perspective.name,
                bpmn_xml: perspective.bpmn_diagram,
                statistics: processDb.statistics[perspective.name]
            })
            if (aggregation_job != 'not_found') {
                aggregation_job?.['payload']?.['job']['extract']
            }
        });
        var response = {
            module: MODULE_PROCESS_TYPE_DETAIL,
            payload: {
                result: 'ok',
                process_type: processDb.process_type,
                historic: {
                    bpmn_job_cnt: processDb.bpmn_job_cnt,
                    instance_cnt: processDb.instance_cnt,
                    perspectives: perspectives
                },
                real_time: {
                    perspectives: aggregation_job?.['payload']?.['job']['extract'] || []
                },
                statistics: {
                    bpmn_job_cnt: processDb.bpmn_job_cnt,
                    instance_cnt: processDb.instance_cnt,
                    perspectives: perspectives
                }
            }
        }
        resolve(response)
    });
    return promise
}

/**
 * Creating a new Artifact in the Database
 * @param {Object} artifact Object containing all datafields necessary for the new Artifact 
 * @returns Promise containing the result of the operation ('already_exists'/'created'/'backend_error')
 */
async function createNewArtifact(artifact,) {
    var promise = new Promise(async function (resolve, reject) {
        var response = {
            module: MODULE_ARTIFACTS,
            payload: {
                type: 'create',
            }
        }
        DDB.readArtifactDefinition(artifact.type, artifact.id).then((result) => {
            if (result != undefined) {
                response.payload.result = 'already_exists'
                resolve(response)
                return
            }
            else {
                DDB.writeNewArtifactDefinition(artifact.type, artifact.id, artifact.stakeholders, artifact.host, artifact.port).then((result) => {
                    if (result == 'error') {
                        response.payload.result = 'backend_error'
                    }
                    else {
                        response.payload.result = 'created'
                    }
                    resolve(response)
                })
            }
        })
    });
    return promise
}

/**
 * Creates a new Stakeholder in the Database
 * @param {String} stakeholder_name Name of the Stakeholder
 * @returns Promise containing the result of the operation ('create'/'already_exists'/'backend_error')
 */
async function createNewStakeholder(stakeholder_name) {
    var promise = new Promise(async function (resolve, reject) {
        var response = {
            module: MODULE_STAKEHOLDERS,
            payload: {
                type: 'create',
            }
        }
        DDB.readStakeholder(stakeholder_name).then((result) => {
            if (result != undefined) {
                response.payload.result = 'already_exists'
                resolve(response)
                return
            }
            else {
                DDB.writeNewStakeholder(stakeholder_name, '').then((result) => {
                    if (result == 'error') {
                        response.payload.result = 'backend_error'
                    }
                    else {
                        response.payload.result = 'created'
                    }
                    resolve(response)
                })
            }
        })
    });
    return promise
}

/**
 * Defines a new Process Group in the Database
 * @param {String} group_id Name of the new Process Group
 * @param {Object} rules Object containing the rules of the freshly created Process Group
 * @returns Promise will contain the result of the operation ('created'/'already_exists'/'backend_error')
 */
async function createNewProcessGroup(group_id, rules) {
    var promise = new Promise(async function (resolve, reject) {
        var response = {
            module: MODULE_NEW_PROCESS_GROUP,
            payload: {
                type: 'create',
            }
        }
        DDB.readProcessGroup(group_id).then((result) => {
            if (result != undefined) {
                response.payload.result = 'already_exists'
                resolve(response)
                return
            }
            else {
                DDB.writeNewProcessGroup(group_id, JSON.stringify(rules)).then((result) => {
                    if (result == 'error') {
                        response.payload.result = 'backend_error'
                    }
                    else {
                        response.payload.result = 'created'
                    }
                    resolve(response)
                })
            }
        })
    });
    return promise
}

/**
 * Creates a new Process Instance. It will create at least one eGSM engines on a Worker selected by the slot selecting policy
 * If the process has multiple perspectives, it will create multiple eGSM engines (not necessarily on the same Worker)
 * Function also publishes to Process Lifecycle topic in case of successful operation
 * @param {String} process_type Type of the process (need to be defined in the library module in advance)
 * @param {String} instance_name Process instance name (the function checks for global uniqueness among other Processes and returns error if the ID is already in use)
 * @param {Boolean} bpmnJob True if an eGSM to BPMN translation Job should be started as well (on an Aggregator)
 * @returns Promise will become 'ok' if the creation was successfull 'id_not_free' if the ID is already in use, 'engines_ok' if translation Job was requested but not managed to create for any reason
 */
async function createProcessInstance(process_type, instance_name, bpmn_job) {
    var promise = new Promise(async function (resolve, reject) {
        MQTTCOMM.searchForProcess(instance_name).then(async (result) => {
            var response = {
                module: MODULE_NEW_PROCESS_INSTANCE,
                payload: {
                    result: 'backend_error',
                }
            }
            if (result.length > 0) {
                response.payload.result = "id_not_free"
                resolve(response)
            }
            else {
                DDB.readProcessType(process_type).then(async (processEntry) => {
                    var processDetails = processEntry.definition
                    var creation_results = []
                    var monitoredEngines = []
                    
                    processDetails['perspectives'].forEach(async element => {
                        var engineName = process_type + '/' + instance_name + '__' + element['name']
                        creation_results.push(MQTTCOMM.createNewEngine(engineName, element['info_model'], element['egsm_model'], element['bindings'], processDetails.stakeholders))
                        monitoredEngines.push(engineName)
                    });

                    await Promise.all(creation_results).then(async (promise_array) => {
                        var aggregatedResult = true
                        promise_array.forEach(element => {
                            if (element != "created") {
                                aggregatedResult = false
                            }
                        });

                        if (aggregatedResult) {
                            var job_results = []
                            
                            // 1. Check if deviation aggregation job exists, create it if needed
                            var deviationAggJobId = process_type + '/deviation_aggregation_job'
                            var existingAggJob = await MQTTCOMM.searchForJob(deviationAggJobId)
                            
                            if (existingAggJob === 'not_found') {
                                var deviationJobConfig = {
                                    id: deviationAggJobId,
                                    type: 'process-deviation-aggregation',
                                    processtype: process_type,
                                    perspectives: processDetails['perspectives'],
                                    notificationrules: 'NOTIFY_ALL'
                                }
                                job_results.push(createJobInstance(deviationAggJobId, deviationJobConfig))
                            } else {
                                job_results.push(Promise.resolve({
                                    payload: { result: "created" }
                                }))
                            }

                            // 2. Create BPMN job
                            if (bpmn_job) {
                                var jobId = process_type + '/' + instance_name + '/bpmn_job'
                                var jobConfig = {
                                    id: jobId,
                                    type: 'bpmn-job',
                                    process_type: process_type,
                                    monitored: monitoredEngines,
                                    perspectives: processDetails['perspectives'],
                                    notificationrules: 'NOTIFY_ALL'
                                }
                                job_results.push(createJobInstance(jobId, jobConfig))
                            }

                            await Promise.all(job_results).then(async (job_creation_results) => {
                                // 3. Notify aggregation jobs about new instance
                                await MQTTCOMM.emitProcessInstanceCreation(process_type, instance_name)
                                
                                var jobAggregatedResult = true
                                job_creation_results.forEach(element => {
                                    if (element.payload.result != "created") {
                                        jobAggregatedResult = false
                                    }
                                });
                                
                                if (aggregatedResult && jobAggregatedResult) {
                                    response.payload.result = "ok"
                                    //Publish message to 'lifecycle' topic to notify aggregators about the new instance
                                    DDB.increaseProcessTypeInstanceCounter(process_type)
                                    resolve(response)
                                }
                                //Engines are created, but the requested related jobs are not ok
                                else if (aggregatedResult && !jobAggregatedResult) {
                                    response.payload.result = "engines_ok"
                                    //Publish message to 'lifecycle' topic to notify aggregators about the new instance
                                    DDB.increaseProcessTypeInstanceCounter(process_type)
                                    DDB.increaseProcessTypeBpmnJobCounter(process_type)
                                    resolve(response)
                                }
                                else {
                                    response.payload.result = "backend_error"
                                    //Make sure we clean up in case any member engine has been created
                                    deleteProcessInstance(process_type, instance_name).then(() => {
                                        resolve(response)
                                    })
                                }
                            })

                        }
                    })
                })
            }
        })
    })
    return promise
}

/**
 * Creates a new job instance on an Aggregator instance selected based on the slot policy
 * @param {String} jobid Id of the new Job (must be globally unique, function checks for uniqueness)
 * @param {Object} jobconfig Object containing all attributes necessary to start the Job
 * @returns Promise containing the result of the operation ('backend_error'/'id_not_free', etc...)
 */
function createJobInstance(jobid, jobconfig) {
    var promise = new Promise(async function (resolve, reject) {
        MQTTCOMM.searchForJob(jobid).then(async (result) => {
            var response = {
                module: MODULE_NEW_PROCESS_INSTANCE,
                payload: {
                    type: 'create',
                    result: 'backend_error',
                }
            }
            if (result != 'not_found') {
                response.payload.result = "id_not_free"
                return resolve(response)
            }
            jobconfig['id'] = jobid
            var result = await MQTTCOMM.createNewJob(jobconfig)
            response.payload.result = result
            return resolve(response)
        })
    })
    return promise
}

/**
 * Get all available process deviation aggregation jobs
 * @returns Promise containing available aggregations
 */
async function getAvailableAggregations() {
    var promise = new Promise(async function (resolve, reject) {
        MQTTCOMM.getDeviationAggregators().then(async (result) => {
            var response = {
                module: MODULE_AGGREGATORS,
                payload: {
                    type: 'available_aggregations',
                    available_aggregations: result
                }
            }
            return resolve(response)
        })
    })
    return promise
}

/**
 * Get complete aggregation data for a specific process type
 * @param {string} processType Process type to get aggregation data for
 * @returns Promise containing complete aggregation data
 */
async function getCompleteAggregationData(processType) {
    var promise = new Promise(async function (resolve, reject) {
        try {
            var jobId = processType + '/deviation_aggregation_job'
            var jobData = await MQTTCOMM.getJobCompleteData(jobId)

            if (jobData && jobData !== 'not_found') {
                var response = {
                    module: MODULE_AGGREGATORS,
                    payload: {
                        complete_aggregation_data: jobData
                    }
                }
            } else {
                var response = {
                    module: MODULE_AGGREGATORS,
                    payload: {
                        complete_aggregation_data: null,
                        error: 'Aggregation job not found'
                    }
                }
            }
            resolve(response)
        } catch (error) {
            LOG.logSystem('ERROR', `Error getting aggregation data: ${error.message}`, module.id)
            var response = {
                module: MODULE_AGGREGATORS,
                payload: {
                    complete_aggregation_data: null,
                    error: error.message
                }
            }
            resolve(response)
        }
    });
    return promise
}

// NOTE: Most functions of the module are intended to use internally only, so they are not exposed here
module.exports = {
    createProcessInstance: createProcessInstance
}