// Read the process parameter
// 
// Command to run this script : 
// pm2 start index.js --no-autorestart --node-args="--max-old-space-size=30000 index.js eventsCount dispatchTrace topic impression search log device_summary kafkapartition"
//Example: pm2 start index.js --no-autorestart --node-args="--max-old-space-size=30000 index.js 600 false loadtest.secor.analytics.raw 200 0 0 0 8"
// 
let data = require('./data');
let faker = require('faker');
var async = require("async");
var deviceList = require('./deviceList')
let eventsToBeGenerated = process.argv[2];
let trace = process.argv[3];
console.log("eventsToBeGenerated" + eventsToBeGenerated)
let events = [];
let batchSize = 200;
let impression = process.argv[5];
let search = process.argv[6];
let log = process.argv[7];
let device_summary = process.argv[8];
let ratio = { impression: impression, search: search, log: log, device_summary: device_summary };
let loops = eventsToBeGenerated / batchSize;
var kafkaDispatcher = require('./kafkaDispatcher')
require('events').EventEmitter.defaultMaxListeners = 10000
var pdata_app_id_count = 0;
var pdata_app_other_count = 0;
let topic = process.argv[4];
let key = "ingest"
var pidMap = {
    "1": "prod.diksha.portal",
    "2": "prod.diksha.app"
}

function getEvent(type, pidIndex) {
    let event = data[type];
    if (type === "SEARCH") {
        event.edata.filters.dialcodes = faker.random.arrayElement(data.dialCodes)
    }
    if (pidIndex === 1) {
        pdata_app_other_count++;
        event.context.pdata.id = pidMap["1"]
    } else {
        event.context.pdata.id = pidMap["2"]
        pdata_app_id_count++;
    }
    event.mid = "LOAD_TEST_" + process.env.machine_id + "_" + faker.random.uuid()
    event.context.did = faker.random.arrayElement(deviceList.dids);
    event.context.channel = faker.random.arrayElement(data.channelIds);
    if (event.object) {
        event.object.id = faker.random.arrayElement(data.contentIds);
    }
    return event;
}

function generateBatch(random_index, cb) {

    for (let i = 0; i < ratio.log; i++) {
        events.push(JSON.parse(JSON.stringify(getEvent('LOG', random_index))));
    }
    for (let i = 0; i < ratio.impression; i++) {
        events.push(JSON.parse(JSON.stringify(getEvent('IMPRESSION', random_index))));
    }
    for (let i = 0; i < ratio.search; i++) {
        events.push(JSON.parse(JSON.stringify(getEvent('SEARCH', random_index))));
    }
    for (let i = 0; i < ratio.device_summary; i++) {
        events.push(JSON.parse(JSON.stringify(getEvent('ME_DEVICE_SUMMARY', random_index))));
    }
    if (events.length >= batchSize) {
        let isBatch = topic.includes(key) ? true : false
        dispatch(events.splice(0, batchSize), isBatch, random_index,
            function(err, res) {
                if (err) {
                    console.error("error occur" + err)
                    cb(err, undefined)
                } else {
                    if (cb) cb(undefined, res)
                }
            })
    } else {
        if (cb) cb()
    }
}

function dispatch(message, batch, random_index, cb) {
    console.log("pdata_app_count" + pdata_app_id_count)
    console.log("pdata_other_count" + pdata_app_other_count)
    if (batch) {
        kafkaDispatcher.dispatchBatch(message, random_index,
            function(err, res) {
                if (err) {
                    console.log('error', err);
                    cb(err, undefined)
                } else {
                    cb(undefined, res)
                }
            })
    } else {
        (async function loop() {
            for (let i = 0; i < message.length; i++) {
                await new Promise(resolve => kafkaDispatcher.dispatch(message[i], function() {
                    resolve(cb())
                }));
            }
        })();
    }
}



function getTraceEvents() {
    var traceEvents = require("./tracerEvents")
    updatedTracerEvents = []
    traceEvents.forEach(function(e) {
        e.mid = "LOAD_TEST_" + process.env.machine_id + "_" + faker.random.uuid() + "_TRACE"
        e.ets = new Date().getTime()
        e.did = faker.random.uuid()
        updatedTracerEvents.push(JSON.parse(JSON.stringify(e)))
    })
    return updatedTracerEvents;
}


// (async function loop() {
//     console.log("Generating IMPRESSION:" + impression + " LOG: " + log + " SEARCH: " + search)
//     for (let i = 1; i <= loops; i++) {
//         await new Promise(resolve => generateBatch(function() {
//             resolve()
//         }));
//     }
//     if (trace === "true") {
//         let isBatch = topic.includes(key) ? true : false
//         var tracerEvents = getTraceEvents()
//         dispatch(tracerEvents, isBatch, function(err, res) {
//             if (!err) {
//                 console.log("Event pushed")
//             } else {
//                 console.error("Error occur due to" + err)
//             }
//         })
//     }
// })();

var random_index = 1;
var time = setInterval(function() {
    generateBatch(random_index, function() {
        console.log("Events pushed")
    }, 100)
})
setTimeout(function() {
    random_index = 2
}, 1000)

setTimeout(function() {
    clearInterval(time)
}, 4000)


// (async function loop() {
//     console.log("Generating IMPRESSION:" + impression + " LOG: " + log + " SEARCH: " + search)
//     setInterval(function() {
//         await new Promise(resolve => generateBatch(function() {
//             resolve()
//         }));
//     }, 100)
// })();