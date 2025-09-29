/**
 * Module implements high-level functions for MQTT-based communication with other parts of the architecture
 */

var UUID = require("uuid");
var EventEmitter = require('events');

var MQTT = require('../egsm-common/communication/mqttconnector')
var AUX = require('../egsm-common/auxiliary/auxiliary')
var LOG = require('../egsm-common/auxiliary/logManager')

//Topic definitions
const WORKERS_TO_SUPERVISOR = 'workers_to_supervisor'
const SUPERVISOR_TO_WORKERS = 'supervisor_to_workers'

const AGGREGATORS_TO_SUPERVISOR = 'aggregators_to_supervisor'
const SUPERVISOR_TO_AGGREGATORS = 'supervisor_to_aggregators'

var BROKER = undefined

//Waiting periods for different operation types
const FREE_SLOT_WAITING_PERIOD = 250
const ENGINE_SEARCH_WAITING_PERIOD = 500
const ENGINE_PONG_WAITING_PERIOD = 500
const PROCESS_SEARCH_WAITING_PERIOD = 500
const DELETE_ENGINE_WAITING_PERIOD = 2000
const AGGREGATOR_PONG_WAITING_PERIOD = 500

//Containers to store pending requests and store replies in case of multi-party cooperation
var REQUEST_PROMISES = new Map() // Request id -> resolve references
var REQUEST_BUFFERS = new Map() // Request id -> Usage specific storage place (used only for specific type of requests)

var NOTIFICATION_TOPICS = new Map() //TOPICS where the SocketServer subscribed to

const EVENT_EMITTER = new EventEmitter()

module.id = 'MQTTCOMM'

/**
 * Message handler function for MQTT messages
 * It neglects the messages not intended to handle in this module
 */
/**
 * Handles incoming MQTT messages
 * It neglects the messages not intended to handle in this module
 * Message body may contain:
 * sender_type: WORKER/AGGREGATOR
 * sender_id: <string>
 * message_type: PONG/PING/NEW_WORKER/NEW_ENGINE_SLOT/NEW_ENGINE_SLOT_RESP/NEW_ENGINE/SEARCH/...
 * request_id: <string> (optional if no response expected)
 * payload: <string>
 * @param {String} hostname Broker host the message received from
 * @param {Number} port Broker port the message received from
 * @param {String} topic Topic the message received from
 * @param {String} message The message itself in string format (considering valid JSON)
 */
function onMessageReceived(hostname, port, topic, message) {
    LOG.logSystem('DEBUG', 'onMessageReceived function called', module.id)
    var msgJson = JSON.parse(message.toString())
    //Messages from the Workers
    if (topic == WORKERS_TO_SUPERVISOR) {
        switch (msgJson['message_type']) {
            case 'NEW_ENGINE_SLOT_RESP': {
                LOG.logSystem('DEBUG', `NEW_ENGINE_SLOT_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_BUFFERS.get(msgJson['request_id']).push({ sender_id: msgJson['sender_id'], free_slots: msgJson['free_slots'] })
                }
                break;
            }
            case 'PONG': {
                LOG.logSystem('DEBUG', `PONG engine message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    if (REQUEST_BUFFERS.has(msgJson['request_id'])) {
                        var filteredMessage = {
                            name: msgJson['sender_id'],
                            engine_mumber: msgJson['payload']['engine_mumber'],
                            capacity: msgJson['payload']['capacity'],
                            uptime: msgJson['payload']['uptime'],
                            hostname: msgJson['payload']['hostname'],
                            port: msgJson['payload']['port']
                        }
                        REQUEST_BUFFERS.get(msgJson['request_id']).push(filteredMessage)
                    }
                }
                break;
            }
            case 'SEARCH': {
                LOG.logSystem('DEBUG', `SEARCH engine message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson)
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            }
            case 'GET_COMPLETE_DIAGRAM_RESP':
                LOG.logSystem('DEBUG', `GET_COMPLETE_DIAGRAM_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload']['result'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            case 'GET_COMPLETE_NODE_DIAGARM_RESP':
                LOG.logSystem('DEBUG', `GET_COMPLETE_NODE_DIAGARM_RESP engine message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload']['result'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            case 'PROCESS_SEARCH_RESP': {
                LOG.logSystem('DEBUG', `PROCESS_SEARCH message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    if (REQUEST_BUFFERS.has(msgJson['request_id'])) {
                        REQUEST_BUFFERS.get(msgJson['request_id']).push(...msgJson['payload']['engines'])
                    }
                }
                break;
            }
            case 'DELETE_ENGINE_RESP': {
                LOG.logSystem('DEBUG', `DELETE_ENGINE_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload']['result'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            }
            case 'GET_ENGINE_LIST_RESP': {
                LOG.logSystem('DEBUG', `GET_ENGINE_LIST_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
            }
        }
    }
    //Messages from an Aggregator
    else if (topic == AGGREGATORS_TO_SUPERVISOR) {
        switch (msgJson['message_type']) {
            case 'PONG': {
                LOG.logSystem('DEBUG', `PONG aggregator message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    if (REQUEST_BUFFERS.has(msgJson['request_id'])) {
                        var filteredMessage = {
                            name: msgJson['sender_id'],
                            activity_mumber: msgJson['payload']['activity_mumber'],
                            uptime: msgJson['payload']['uptime'],
                            hostname: msgJson['payload']['hostname'],
                            port: msgJson['payload']['port'],
                            capacity: msgJson['payload']['capacity']
                        }
                        REQUEST_BUFFERS.get(msgJson['request_id']).push(filteredMessage)
                    }
                }
                break;
            }
            case 'NEW_JOB_SLOT_RESP': {
                LOG.logSystem('DEBUG', `NEW_JOB_SLOT message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_BUFFERS.get(msgJson['request_id']).push({ sender_id: msgJson['sender_id'], free_slots: msgJson['free_slots'] })
                }
                break;
            }
            case 'SEARCH': {
                LOG.logSystem('DEBUG', `SEARCH job message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson)
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            }
            case 'GET_DEVIATION_AGGREGATORS_RESP': {
                LOG.logSystem('DEBUG', `GET_DEVIATION_AGGREGATORS_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            }
            case 'GET_COMPLETE_JOB_DATA_RESPONSE': {
                LOG.logSystem('DEBUG', `GET_DEVIATION_AGGREGATORS_RESP message received, request_id: [${msgJson['request_id']}]`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])(msgJson['payload'])
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
            }
        }
    }
    else if (NOTIFICATION_TOPICS.has(topic)) {
        EVENT_EMITTER.emit('notification', topic, msgJson)
    }
}

/**
 * Inits MQTT broker connection and subscribes to the necessary topics to start operation
 * @param {Broker} broker Broker the supervisor should use to reach out to the managed workers and aggregators
 */
async function initBrokerConnection(broker) {
    BROKER = broker
    LOG.logSystem('DEBUG', `initBrokerConnection function called`, module.id)

    MQTT.init(onMessageReceived)
    MQTT.createConnection(BROKER.host, BROKER.port, BROKER.username, BROKER.password)
    await MQTT.subscribeTopic(BROKER.host, BROKER.port, WORKERS_TO_SUPERVISOR)
    await MQTT.subscribeTopic(BROKER.host, BROKER.port, AGGREGATORS_TO_SUPERVISOR)

    LOG.logSystem('DEBUG', `initBrokerConnection function ran successfully`, module.id)
}

/**
 * Helper function to perform waiting for responses
 * @param {Number} delay Delay period in millis 
 */
async function wait(delay) {
    await AUX.sleep(delay)
}

/**
 * Intended to find a free engine slot on any worker instance
 * @returns Returns with a promise whose value will be 'no_response' in case of no free slot found and 
 * it will contain the ID of a Worker with a free slot otherwise
 * If more than one response arrived (multiple Workers has free slot) then the function will choose the one with the most free slots
 */
async function getFreeEngineSlot() {
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'NEW_ENGINE_SLOT'
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))

    var promise = new Promise(async function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, [])
        await wait(FREE_SLOT_WAITING_PERIOD)
        LOG.logSystem('DEBUG', `getFreeEngineSlot waiting period elapsed`, module.id)
        var result = REQUEST_BUFFERS.get(request_id) || []
        REQUEST_PROMISES.delete(request_id)
        REQUEST_BUFFERS.delete(request_id)
        if (result.length == 0) {
            resolve('no_response')
        }
        else {
            //Selecting the worker with the most free slots
            var maxSlots = 0
            var selected = "no_response"
            result.forEach(element => {
                if (element.free_slots > maxSlots) {
                    maxSlots = element.free_slots
                    selected = element.sender_id
                }
            });
            resolve(selected)
        }
    });
    return promise
}

/**
 * Creates a new engine on a random Worker which has at least one free engine slot
 * @param {String} engineid ID of the new engine
 * @param {String} informal_model
 * @param {String} process_model 
 * @param {String} eventRouterConfig 
 * @param {String[]} stakeholders 
 */
function createNewEngine(engineid, informal_model, process_model, eventRouterConfig, stakeholders) {
    LOG.logSystem('DEBUG', `createNewEngine function called with engine ID: [${engineid}]`, module.id)
    var promise = new Promise(function (resolve, reject) {
        getFreeEngineSlot().then((value) => {
            if (value != 'no_response') {
                LOG.logSystem('DEBUG', `Free engine slot found on Worker: [${value}]`, module.id)
                var msgPayload = {
                    "engine_id": engineid,
                    "mqtt_broker": BROKER.host,
                    "mqtt_port": BROKER.port,
                    "mqtt_user": BROKER.username,
                    "mqtt_password": BROKER.password,
                    "informal_model": informal_model,
                    "process_model": process_model,
                    "event_router_config": eventRouterConfig,
                    "stakeholders": stakeholders
                }
                var requestId = UUID.v4();
                var message = {
                    request_id: requestId,
                    message_type: 'NEW_ENGINE',
                    payload: msgPayload
                }
                MQTT.publishTopic(BROKER.host, BROKER.port, value, JSON.stringify(message))
                resolve("created")
            }
            else {
                LOG.logSystem('WARNING', `No free engine slot found`, module.id)
                resolve("no_free_slot")
            }
        })
    })
    return promise
}

/**
 * Finds the location of a specified engine instance
 * @param {String} engineid Engine id of the engine intended to be found
 * @returns A Promise, which contains the Worker ID, its hostname and RESP API port number, where the engine is placed,
 * If not found its value will be 'not_found' 
 */
async function searchForEngine(engineid) {
    LOG.logSystem('DEBUG', `Searching for Engine [${engineid}]`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'SEARCH',
        "payload": { "engine_id": engineid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_SEARCH_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Finds Engine instances belonging to the specified Process instance
 * @param {String} processid Instance ID of the process
 * @returns Promise will contain an array of Engines sorted based on their name
 */
async function searchForProcess(processid) {
    LOG.logSystem('DEBUG', `Searching for Process [${processid}]`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'PROCESS_SEARCH',
        "payload": { "process_id": processid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, [])
        wait(PROCESS_SEARCH_WAITING_PERIOD).then(() => {
            LOG.logSystem('DEBUG', `searchForProcess waiting period elapsed`, module.id)
            var result = REQUEST_BUFFERS.get(request_id) || []
            REQUEST_PROMISES.delete(request_id)
            REQUEST_BUFFERS.delete(request_id)
            result.sort((a, b) => {
                const nameA = a.name.toUpperCase();
                const nameB = b.name.toUpperCase();
                if (nameA < nameB) {
                    return -1;
                }
                if (nameA > nameB) {
                    return 1;
                }
                return 0;
            });
            var cnt = 1
            result.forEach(element => {
                element['index'] = cnt
                cnt += 1
            });
            resolve(result)
        })
    });
    return promise
}

/**
 * Delete a specified engine
 * @param {string} engineid 
 * @returns A promise to the result of the operaton. Will contain Worker defined content or "delete_error" in case of no Worker response
 */
async function deleteEngine(engineid) {
    LOG.logSystem('DEBUG', `deleteEngine called for [${engineid}]`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'DELETE_ENGINE',
        "payload": { "engine_id": engineid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(DELETE_ENGINE_WAITING_PERIOD).then(() => {
            LOG.logSystem('DEBUG', `searchForProcess waiting period elapsed for deleteEngine`, module.id)
            resolve("delete_error")
        })
    });
    return promise
}

/**
 * Deletes all engines belonging to the specified Process Instance
 * @param {string} processid 
 * @returns A promise to the operation result. Will contain "ok", or "error" (considered to be error if not all engines has been verified to be removed)
 */
async function deleteProcess(processid) {
    var promise = new Promise(async function (resolve, reject) {
        searchForProcess(processid).then(async (engines) => {
            var results = []
            engines.forEach(element => {
                results.push(deleteEngine(element['name']))
            });
            Promise.all(results).then((results) => {
                results.forEach(element => {
                    if (element != "deleted") {
                        resolve('error')
                    }
                });
                resolve("ok")
            })
        })
    })
    return promise
}

/**
 * Returns the list of online Worker instances
 * @returns Returns a promise, which will contain the list of Workers after ENGINE_PONG_WAITING_PERIOD
 */
async function getWorkerList() {
    LOG.logSystem('DEBUG', `getWorkerList function called`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'PING'
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, [])
        wait(ENGINE_PONG_WAITING_PERIOD).then(() => {
            LOG.logSystem('DEBUG', `getWorkerList waiting period elapsed`, module.id)
            var result = REQUEST_BUFFERS.get(request_id) || []
            REQUEST_PROMISES.delete(request_id)
            REQUEST_BUFFERS.delete(request_id)
            result.sort((a, b) => {
                const nameA = a.name.toUpperCase();
                const nameB = b.name.toUpperCase();
                if (nameA < nameB) {
                    return -1;
                }
                if (nameA > nameB) {
                    return 1;
                }
                return 0;
            });
            var cnt = 1
            result.forEach(element => {
                element['index'] = cnt
                cnt += 1
            });
            resolve(result)
        })
    });
    return promise
}

/**
 * Get the list of Engines deployed on a specified Worker
 * @param {String} workername 
 * @returns 
 */
function getWorkerEngineList(workername) {
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'GET_ENGINE_LIST',
        "payload": {}
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, workername, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, undefined)
        wait(ENGINE_PONG_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Get eGSM model of an engine to visualize on Front end
 * @param {String} engineid 
 * @returns 
 */
function getEngineCompleteDiagram(engineid) {
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'GET_COMPLETE_DIAGRAM',
        "payload": { engine_id: engineid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_PONG_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Get eGSM model of an engine to visualize on Front end
 * @param {String} engineid 
 * @returns 
 */
function getEngineCompleteNodeDiagram(engineid) {
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'GET_COMPLETE_NODE_DIAGARM',
        "payload": { engine_id: engineid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_WORKERS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_PONG_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

// Aggregator-related functions
/**
 * Creates a new Monitoring activity on a specified Aggregator
 * @param {String} activityid Desired Job ID
 * @param {Object} definition Job definition object, containing all required fields 
 * @param {String} aggregatorid ID of desired Aggregator
 * @returns 
 */
function createNewMonitoringActivity(activityid, definition, aggregatorid) {
    LOG.logSystem('DEBUG', `createNewMonitoringActivity function called`, module.id)
    var promise = new Promise(function (resolve, reject) {
        var request_id = UUID.v4();
        var message = {
            "request_id": request_id,
            "message_type": 'NEW_MONITORING_ACTIVITY',
            "payload": {
                "activity_id": activityid,
                "definition": definition
            }
        }
        MQTT.publishTopic(BROKER.host, BROKER.port, aggregatorid, JSON.stringify(message))
        resolve("created")
    });
    return promise
}

/**
 * Returns the list of online Aggregator instances
 * @returns Returns a promise, which will contain the list of Aggregators sorted by names
 */
async function getAggregatorList() {
    LOG.logSystem('DEBUG', `getAggregatorList function called`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'PING'
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, [])
        wait(AGGREGATOR_PONG_WAITING_PERIOD).then(() => {
            LOG.logSystem('DEBUG', `getAggregatorList waiting period elapsed`, module.id)
            var result = REQUEST_BUFFERS.get(request_id) || []
            REQUEST_PROMISES.delete(request_id)
            REQUEST_BUFFERS.delete(request_id)
            result.sort((a, b) => {
                const nameA = a.name.toUpperCase();
                const nameB = b.name.toUpperCase();
                if (nameA < nameB) {
                    return -1;
                }
                if (nameA > nameB) {
                    return 1;
                }
                return 0;
            });
            var cnt = 1
            result.forEach(element => {
                element['index'] = cnt
                cnt += 1
            });
            resolve(result)
        })
    });
    return promise
}

/**
 * Getting free Job Slots on online Aggregators
 * @returns Returns a Promise, which will contain an Aggregator ID with at least one free Job slot, or 'no_response' in case of no free slot
 */
async function getFreeJobSlot() {
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'NEW_JOB_SLOT'
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(message))

    var promise = new Promise(async function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        REQUEST_BUFFERS.set(request_id, [])
        await wait(FREE_SLOT_WAITING_PERIOD)
        LOG.logSystem('DEBUG', `getFreeJobSlot waiting period elapsed`, module.id)
        var result = REQUEST_BUFFERS.get(request_id) || []
        REQUEST_PROMISES.delete(request_id)
        REQUEST_BUFFERS.delete(request_id)
        if (result.length == 0) {
            resolve('no_response')
        }
        else {
            var maxSlots = 0
            var selected = "no_response"
            result.forEach(element => {
                if (element.free_slots > maxSlots) {
                    maxSlots = element.free_slots
                    selected = element.sender_id
                }
            });
            resolve(selected)
        }
    });
    return promise
}

/**
 * Creating a new Job instance on an automatically selected Aggregator
 * @param {Object} jobconfig Configuration of the job containing all required fields
 * @returns A promise will contain the result of the operation: 'created'/'no_free_slot'/'error'
 */
async function createNewJob(jobconfig) {
    LOG.logSystem('DEBUG', `createNewJob function called with engine ID`, module.id)
    var promise = new Promise(function (resolve, reject) {
        getFreeJobSlot().then((value) => {
            if (value != 'no_response') {
                LOG.logSystem('DEBUG', `Free job slot found on Aggregator: [${value}]`, module.id)
                var msgPayload = {
                    job_config: jobconfig,
                }
                var requestId = UUID.v4();
                var message = {
                    request_id: requestId,
                    message_type: 'NEW_JOB',
                    payload: msgPayload
                }
                MQTT.publishTopic(BROKER.host, BROKER.port, value, JSON.stringify(message))
                resolve("created")
            }
            else {
                LOG.logSystem('WARNING', `No free job slot found`, module.id)
                resolve("no_free_slot")
            }
        })
    })
    return promise
}

/**
 * Finds a Job instance specified by its ID
 * @param {String} jobid ID of the requested Job
 * @returns A promise will contain information about the Aggregator deploying the job, or 'not_found'
 */
async function searchForJob(jobid) {
    LOG.logSystem('DEBUG', `Searching for Job [${jobid}]`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'SEARCH',
        "payload": { "job_id": jobid }
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_SEARCH_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Get jobs from a specific aggregator
 * @returns Promise containing array of aggregators
 */
async function getDeviationAggregators() {
    LOG.logSystem('DEBUG', `Searching for all deviation aggregators`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'GET_DEVIATION_AGGREGATORS',
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_SEARCH_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Get complete job data including aggregated results
 * @param {string} jobId Job ID to retrieve
 * @returns Promise containing complete job data
 */
async function getJobCompleteData(jobId) {
    LOG.logSystem('DEBUG', `Retrieving aggregated data for for Job [${jobId}]`, module.id)
    var request_id = UUID.v4();
    var message = {
        "request_id": request_id,
        "message_type": 'GET_COMPLETE_JOB_DATA',
        job_id: jobId,
    }
    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)
        wait(ENGINE_SEARCH_WAITING_PERIOD).then(() => {
            resolve('not_found')
        })
    });
    return promise
}

/**
 * Can be used to subscribe to a defined MQTT topic
 * In case of a new message from the topic a new event will be emitted by the EVENT_EMITTER
 * Subscription to the same topic can be performed multiple times, in this case intead of multiply subscription
 * the function wil just increase a counter 
 * @param {String} topic Topic to subscribe
 */
async function subscribeNotificationTopic(topic) {
    if (!NOTIFICATION_TOPICS.has(topic)) {
        await MQTT.subscribeTopic(BROKER.host, BROKER.port, topic)
        NOTIFICATION_TOPICS.set(topic, 1)
    }
    else {
        NOTIFICATION_TOPICS.set(topic, NOTIFICATION_TOPICS.get(topic) + 1)
    }
}

/**
 * Unsubscribe from a topic where the system subscribed to (by subscribeNotificationTopic function)
 * If the subscribeNotificationTopic function has been called multiple times for the same topic, then this
 * function will perform the unsusbscription only after the last subscription has been undone
 * @param {String} topic Topic to unsubscribe from 
 */
async function unsubscribeNotificationTopic(topic) {
    if (NOTIFICATION_TOPICS.has(topic)) {
        if (NOTIFICATION_TOPICS.get(topic) == 1) {
            NOTIFICATION_TOPICS.delete(topic)
            MQTT.unsubscribeTopic(BROKER.host, BROKER.port, topic)
        }
        else {
            NOTIFICATION_TOPICS.set(topic, NOTIFICATION_TOPICS.get(topic) - 1)
        }
    }
}

/**
 * Emit process instance creation notification to all aggregators
 * @param {String} processType Process type of the new instance
 * @param {String} instanceId Instance ID of the new instance
 */
async function emitProcessInstanceCreation(processType, instanceId) {
    LOG.logSystem('DEBUG', `Emitting process instance creation: ${processType}/${instanceId}`, module.id)
    const newInstanceMessage = {
        request_id: UUID.v4(),
        message_type: 'NEW_PROCESS_INSTANCE',
        payload: {
            "process_type": processType,
            "process_id": instanceId
        }
    }

    MQTT.publishTopic(BROKER.host, BROKER.port, SUPERVISOR_TO_AGGREGATORS, JSON.stringify(newInstanceMessage))
}

module.exports = {
    EVENT_EMITTER: EVENT_EMITTER,
    initBrokerConnection: initBrokerConnection,

    createNewEngine: createNewEngine,
    deleteProcess: deleteProcess,
    searchForEngine: searchForEngine,
    searchForProcess: searchForProcess,
    getWorkerList: getWorkerList,
    getWorkerEngineList: getWorkerEngineList,
    getEngineCompleteDiagram: getEngineCompleteDiagram,
    getEngineCompleteNodeDiagram: getEngineCompleteNodeDiagram,
    emitProcessInstanceCreation: emitProcessInstanceCreation,

    createNewMonitoringActivity: createNewMonitoringActivity,
    getAggregatorList: getAggregatorList,

    createNewJob: createNewJob,
    searchForJob: searchForJob,

    subscribeNotificationTopic: subscribeNotificationTopic,
    unsubscribeNotificationTopic: unsubscribeNotificationTopic,

    getDeviationAggregators: getDeviationAggregators,
    getJobCompleteData: getJobCompleteData,
}