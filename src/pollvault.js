var sys = require("sys"),
        http = require("http"),
        url = require("url"),
        path = require("path"),
        fs = require("fs"),
        events = require("events"),
        utils = require('util'),
        querystring = require("querystring"),
        net = require("net");

var aws = require("../lib/aws");

var seqid = 0;

// parameters
var LISTEN_PORT = 8085;
var LISTEN_URL = "http://24.7.76.113";
var POLL_TIMEOUT = 45000;
var TOPIC_TIMEOUT = 60000;
var MESSAGE_HISTORY_MAX = 20;
var SNS_ARN = "none";

var snsStatus = "none"

var snsClient = null;

//
// init
//

loadConfiguration();

var launchStageTwo = function() {
    snsClient = aws.createSNSClient("AKIAIUKB5FCJBAL3CT3A", "GNF6vZoZlJzCCgjl5jlvYA8PnfObhNr9NmwGTwGG", null);

    if (SNS_ARN != "none"){
        subcribeToSNS();
    }

    launch();
}



//
// functions
//

function subcribeToSNS() {
    var query = [];
    query['Endpoint'] = LISTEN_URL + ':' + LISTEN_PORT;
    query['Protocol'] = 'http';
    query['TopicArn'] = SNS_ARN;
    sys.puts(snsClient);
    snsClient.call('Subscribe',query,function(obj){
        if (obj.Error != undefined){
            sys.puts("Error subscribing to SNS: " + sys.inspect(obj));
        } else if (obj.SubscribeResult != undefined) {

        }

        sys.puts("Subscribe result: " + sys.inspect(obj));
    });
}

function loadConfiguration() {
    var filename = path.join(process.cwd(), "pollvaultConfig.json");
    path.exists(filename, function(exists) {
        if (!exists) {
            console.log("ERROR: Could not find configuration:" + path.toString());
            launchStageTwo();
            return;
        }

        fs.readFile(filename, "utf8", function(err, file) {
            if (err) {
                console.log("ERROR: Could not read configuration: " + err)
                return;
            }
            var config = eval("(" + file + ")");
            if (config.listenPort != undefined) {
                LISTEN_PORT = config.listenPort;
            }
            if (config.pollTimeout != undefined) {
                POLL_TIMEOUT = config.pollTimeout;
            }
            if (config.topicTimeout != undefined) {
                TOPIC_TIMEOUT = config.topicTimeout;
            }
            if (config.messageHistoryMax != undefined) {
                MESSAGE_HISTORY_MAX = config.messageHistoryMax;
            }
            if (config.snsArn != undefined) {
                sys.puts("One: " + config.snsArn);
                SNS_ARN = config.snsArn;
            }
            console.log("Configuration file read.")
            launchStageTwo();
        });

    });
}

// this is used to serve static files, like our HTML test pages
function sendStaticFile(uri, response) {
    var filename = path.join(process.cwd(), uri);
    path.exists(filename, function(exists) {
        if (!exists) {
            response.writeHead(404, {"Content-Type": "text/plain"});
            response.write("404 Not Found\n");
            response.end();
            filename = null;
            return;
        }

        fs.readFile(filename, "binary", function(err, file) {
            if (err) {
                response.writeHead(500, {"Content-Type": "text/plain"});
                response.write(err + "\n");
                response.end();
                filename = null;
                return;
            }

            response.writeHead(200);
            response.write(file, "binary");
            response.end();
            filename = null;
        });
    });
}

// all current topics are stored in here
var topics = {};


// function for sending human readable error messages
// note that common error messages cause by bad requests do not go through
// here so we don't get the logs spammed by bad clients.
var sendErrorMessage = function(response, resultCode, resultText, message) {
    console.log("Sending ERROR message [" + resultCode + "] " + message);
    response.writeHead(resultCode, resultText, {'Content-Type': 'text/html'});
    response.write('<html><head><title>[' + resultCode + '] ' + resultText + '  </title></head><body><h1>' + message + '</h1></body></html>')
    response.end();
}
// function for sending plain text responses
var sendMessage = function(response, resultCode, resultText, message) {
    //console.log("Sending message [" + resultCode + "] " + message);
    response.writeHead(resultCode, resultText, {'Content-Type': 'text/html'});
    response.write(message)
    response.end();
}


// This is the ONLY function that sends poll responses.
// (except for errors or the timeout OK)
// It's important to use this method for all sending to trap the case
// where message(s) come in on one or more topic while the client is
// doing a round-trip and has no active listeners
//
// note that with a low count it's possible that a noisy first topic will
// fill the send queue and the client will never receive messages from
// a topic further in the requested topic list. so always list your
// topics in order of priority.
var sendBacklog = function(response, requestTopics, seqidIn, count) {
    // gather the topics we want and check their backlog of messages
    // also keeps track of the largest sequence ID of the messages to send
    var toSend = [];
    var recentSeqid = 0;
    if (count == undefined) {
        count = -1;
    }
    for (var topicIndex in requestTopics) {
        topic = requestTopics[topicIndex];
        if (seqidIn != 0) {
            for (var oldMessageIndex in topic.messageHistory) {
                var oldMessage = topic.messageHistory[oldMessageIndex];
                if (oldMessage.seqid > seqidIn) {
                    toSend.push(oldMessage);
                }
            }
        }
    }

    // we have one or more messages to send
    if (toSend.length > 0) {
        // make sure they're in proper order
        // (since we gathered from more than one topic they could be out of order)
        toSend.sort(function(a, b) {
            return a.seqid - b.seqid;
        });
        // collect the message texts, but only the count we want
        var burn = toSend.length - count + 1;
        var messages = [];
        for (var nmIndex in toSend) {
            if (--burn > 0) {
                continue;
            }
            messages.push(toSend[nmIndex].payload);
            if (toSend[nmIndex].seqid > recentSeqid) {
                recentSeqid = toSend[nmIndex].seqid;
            }
        }
        // send them along
        sendMessage(response, 200, "OK", JSON.stringify(
            {
                seqid : recentSeqid,
                result : "OK",
                message : messages
            }
        ));
        response.end();
        messages = null;
        toSend = null;
        recentSeqid = null;
        return true;
    }
    toSend = null;
    return false;
}

// create a new topic for messaging
var newTopic = function(topicName, seqidNew) {
    var newTopic = topics[topicName] = {
        name : topicName,
        messageHistory : [],
        emitter : new events.EventEmitter(),
        lastSeqid : seqidNew,
        lastMessageTime : Date.now()
    };
    return newTopic;
}

function addMessage(topicName, topic, decodedBody) {
    // update our sequence number so everyone knows there's a new message
    var seqidNew = ++seqid;

    // make sure we have this topic, or create a new one
    if (topics[topicName] == undefined || topics[topicName] == null) {
        topic = newTopic(topicName, -1);
    } else {
        topic = topics[topicName];
    }

    // update the topic information
    topic.lastSeqid = Date.now;
    topic.lastMessageTime = Date.now;

    // create the message
    var newMessage = {
        seqid: seqidNew,
        postTime: Date.now(),
        payload: decodedBody.message
    }

    // add our message and if we're over our max limit pop off the oldest one
    // (unshift pushes us in the front of the list. what the hell kind of name is that?
    //  some kind of assembly hangover here in JavaScript?)
    if (topic.messageHistory.unshift(newMessage) > MESSAGE_HISTORY_MAX) {
        topic.messageHistory.pop();
    }
    // DeVry

    // send an event to all this topics listeners
    topic.emitter.emit("message", decodedBody.message);
}


//
// our server
//

var launch = function() {
    http.createServer(
            function(request, response) {
                var longPoll = true;
                request.setEncoding("utf8");

                // check the path to determine our action
                switch (request.url.split('?')[0]) {
                    case '/stats':
                        sendErrorMessage(response, 501, "Not implemented", JSON.stringify([
                            {
                                seqid : -1,
                                result : "ERROR",
                                message : "Not implemented"
                            }
                        ]));
                        break;
                    case '/postSNS':
                        // accept a post from Amazon SNS - untested as of yet
                        if (request.method == 'POST') {
                            var fullBody = '';

                            request.on('data', function(chunk) {
                                // append the current chunk of data to the fullBody variable
                                fullBody += chunk.toString();
                            });

                            request.on('end', function() {
                                var topic = null;
                                var topicName = '';

                                // parse the received body data
                                var decodedBody = querystring.parse(fullBody);

                                if (decodedBody.subject == undefined) {
                                    sendMessage(response, 200, "OK", JSON.stringify([
                                        {
                                            seqid : seqid,
                                            result : "ERROR",
                                            message : "No subject specified"
                                        }
                                    ]));
                                    fullBody = decodedBody = null;
                                    response = null;
                                    return;
                                } else {
                                    topicName = "" + decodedBody.subject;
                                }

                                if (decodedBody.message == undefined) {
                                    sendMessage(response, 200, "OK", JSON.stringify([
                                        {
                                            seqid : seqid,
                                            result : "ERROR",
                                            message : "No message"
                                        }
                                    ]));
                                    fullBody = decodedBody = null;
                                    response = null;
                                    return;
                                }

                                addMessage(topicName, topic, decodedBody);

                                decodedBody = null;
                                topicName = null;
                                fullBody = null;
                                topic = null;

                                response.end();
                                response = null;
                            });
                        }
                        break;
                    case '/post':
                        // add a message to a topic queue
                        if (request.method == 'POST') {
                            var fullBody = '';

                            // accept a data chunk
                            request.on('data', function(chunk) {
                                // append the current chunk of data to the fullBody variable
                                fullBody += chunk.toString();
                            });

                            // end of the incoming data, process the post
                            request.on('end', function() {
                                var topic = null;
                                var topicName = '';

                                // parse the received body data
                                var decodedBody = querystring.parse(fullBody);

                                if (decodedBody.topic == undefined) {
                                    sendMessage(response, 200, "OK", JSON.stringify([
                                        {
                                            seqid : seqid,
                                            result : "ERROR",
                                            message : "No topic specified"
                                        }
                                    ]));
                                    fullBody = decodedBody = null;
                                    response = null;
                                    return;
                                } else {
                                    topicName = "" + decodedBody.topic;
                                }

                                if (decodedBody.message == undefined) {
                                    sendMessage(response, 200, "OK", JSON.stringify([
                                        {
                                            seqid : seqid,
                                            result : "ERROR",
                                            message : "No message"
                                        }
                                    ]));
                                    response = null;
                                    fullBody = decodedBody = null;
                                    return;
                                }

                                addMessage(topicName, topic, decodedBody);

                                // aid our garbage collection
                                decodedBody = null;
                                fullBody = null;

                                response.end();
                                response = null;
                            });
                        } else {
                            var fullBody = '';

                            // accept a data chunk
                            request.on('data', function(chunk) {
                                // append the current chunk of data to the fullBody variable
                                fullBody += chunk.toString();
                            });

                            // end of the incoming data, process the post
                            request.on('end', function() {
                                var topic = null;
                                var topicName = '';

                                // parse the received body data
                                var decodedBody = querystring.parse(fullBody);

                                sys.puts("Uknown request: " + decodedBody);

                                sendMessage(response, 405, "Method not supported", JSON.stringify([
                                    {
                                        seqid : -1,
                                        result : "ERROR",
                                        message : "Method not supported"
                                    }
                                ]));

                                response.end();
                                response = null;
                            });
                        }
                        break;
                    case '/poll':
                        longPoll = false;
                    case '/longpoll':
                        if (request.method == 'GET') {
                            // on a GET we start a long polling request

                            var fullBody = '';

                            // accept a data chunk
                            request.on('data', function(chunk) {
                                // append the current chunk of data to the fullBody variable
                                fullBody += chunk.toString();
                            });

                            // end of incoming data, process our request
                            request.on('end', function() {
                                var requestTopics = [];
                                var topicNames = [];
                                var seqid = seqid;
                                var count = 200;

                                // we ignore the body of the GET
                                fullBody = null;

                                // parse the received query data
                                var urlObj = url.parse(request.url, true);


                                // get the topic(s) we want to listen to
                                if (urlObj.query["topic"] == undefined) {
                                    sendMessage(response, 200, "OK", JSON.stringify([
                                        {
                                            seqid : seqid,
                                            result : "ERROR",
                                            message : "No topic specified"
                                        }
                                    ]));
                                    response = null;
                                    return;
                                } else {
                                    topicNames = (urlObj.query["topic"]).split(',');
                                }

                                // get the sequence id
                                if (urlObj.query["seqid"] != undefined) {
                                    seqidIn = parseInt(urlObj.query["seqid"]);
                                }

                                // get the count of messages you want
                                if (urlObj.query["count"] != undefined) {
                                    count = parseInt(urlObj.query["count"]);
                                }

                                for (var topicNameIndex in topicNames) {
                                    var topicName = topicNames[topicNameIndex];
                                    if (topics[topicName] == undefined || topics[topicName] == null) {
                                        console.log("Creating new topic for GET '" + topicName + "' ");
                                        topic = newTopic(topicName, -1);
                                    } else {
                                        topic = topics[topicName];
                                    }
                                    requestTopics.push(topic);
                                }

                                // if we have messages waiting for this request send them now
                                // otherwise start the long poll
                                if (!sendBacklog(response, requestTopics, seqidIn, count)) {
                                    // no backlog, if we're not long polling send OK
                                    if (!longPoll) {
                                        // the timeout message
                                        sendMessage(response, 200, "OK", JSON.stringify(
                                            {
                                                seqid : response.mySeqid,
                                                result : "OK",
                                                message : []
                                            }
                                        ));
                                    } else {
                                        // save our seqid so some other event doesn't increment it before we use it
                                        response.mySeqid = seqidIn;
                                        response.topics = {};
                                        // add us as a listener for topic events
                                        for (var topicIndex in requestTopics) {
                                            var topic = requestTopics[topicIndex];
                                            // we need to save the function we're going to use as the
                                            // listener callback because we have to use it, and only it,
                                            // to remove the listener later.
                                            response.topics[topic.name] = function(message) {
                                                //console.log("Listener Fired with message'" + message + "' ");
                                                // the listener tells us there's a new backlog of messages to send,
                                                // so send them
                                                sendBacklog(response, requestTopics, response.mySeqid, count);
                                                // no go through and remove all of our listeners because this request is over
                                                for (var topicIndex in requestTopics) {
                                                    var topic = requestTopics[topicIndex];
                                                    topic.emitter.removeListener("message", response.topics[topic.name]);
                                                }
                                                // make sure to stop our timeout as well
                                                clearTimeout(response.myTimeout);
                                                // all done
                                                response.end();
                                                response = null;
                                                requestTopics = null;
                                            }

                                            // now we add the listener
                                            var listener = topic.emitter.addListener("message", response.topics[topic.name]);
                                        }

                                        // don't forget a timeout
                                        response.myTimeout = setTimeout(function() {
                                            // the timeout message
                                            sendMessage(response, 200, "OK", JSON.stringify(
                                                {
                                                    seqid : response.mySeqid,
                                                    result : "OK",
                                                    message : []
                                                }
                                            ));
                                            // make sure to remove our listeners after a timeout
                                            for (var topicIndex in requestTopics) {
                                                var topic = requestTopics[topicIndex];
                                                topic.emitter.removeListener("message", response.topics[topic.name]);
                                            }
                                            response = null;
                                            requestTopics = null;
                                        }, POLL_TIMEOUT);
                                    }
                                }
                            });
                        } else {
                            sendMessage(response, 405, "Method not supported", JSON.stringify([
                                {
                                    seqid : -1,
                                    result : "ERROR",
                                    message : "Method not supported"
                                }
                            ]));
                        }
                        break;
                    default:
                        sendStaticFile(request.url.split('?')[0], response);
                };

                // the dead topic sweeper
                setInterval(function() {
                    var now = Date.now();
                    var deadTopics = [];
                    for (var topicIndex in topics) {
                        var topic = topics[topicIndex];
                        if (now - topic.lastMessageTime > TOPIC_TIMEOUT
                            && topic.emitter.listeners.length == 0) {
                            deadTopics.push(topic);
                        }
                    }
                    for (topicIndex in deadTopics) {
                        var topic = deadTopics[topicIndex];
                        delete topics[topic.name];
                    }
                }, 10000);

            }).listen(LISTEN_PORT);

    sys.puts("Server running at http://localhost:" + LISTEN_PORT + "/");
}

