const Redis = require("ioredis");
var constants = require("./constants");
var Message = require("./Message");
const fs = require('fs');
const os = require('os');
var processId = os.hostname() + "-" + Math.random().toString(36).substring(7);

var myargs = process.argv.slice(2); // channelName, subscriberId
var subscriberFileName = myargs[0];
const { hostName, port, pwd, appInsightKey } = require("./config");

const appInsights = require("applicationinsights");
const MessageReceived = require("./MessageReceived");
appInsights.setup(appInsightKey).start();
var client = appInsights.defaultClient;
const nodes = [
  {
    port: port,
    host: hostName,
  },
];
const sub = new Redis.Cluster(nodes, {
  showFriendlyErrorStack: true,
  maxRetriesPerRequest: 3,
  enableAutoPipelining: true,
  connectTimeout: 20000,
  password: pwd,
  slotsRefreshTimeout: 5000,
  enableOfflineQueue: false,
  enableReadyCheck: false,
  dnsLookup: (address, callback) => callback(null, address),
  redisOptions: {
    family: 4,
    tls: {
      servername: hostName,
    },
    showFriendlyErrorStack: true,
    maxRetriesPerRequest: 3,
    enableAutoPipelining: true,
    connectTimeout: 20000,
    password: pwd,
    slotsRefreshTimeout: 5000,
    enableOfflineQueue: false,
    enableReadyCheck: false,
  },
});
/*
const sub = new Redis({
  port: port,
  host: "p4redis.redis.cache.windows.net",
  family: 4,
  password: pwd,
  connectTimeout: 20000,
  tls: {
    servername: "p4redis.redis.cache.windows.net",
  },
});
*/

var index = 0;
var channelList = new Set();
var channelToIndexMap = new Map();
var indexToChannelMap = new Map();

sub.on("reconnecting", function () {
  var propertySet = {
    errorMessage: "Reconnecting redis",
    descriptiveMessage: "Redis reconnection event called"
  };
  client.trackEvent({ name: "redisSubConnMsg", properties: propertySet });
  client.trackMetric({ name: "redisSubReconnect", value: 1.0 });
  // console.log("reconnecting")
});

sub.on("ready", function () {
  var propertySet = {
    errorMessage: "null",
    descriptiveMessage: "Redis Connection ready. Starting execution"
  };
  client.trackEvent({ name: "redisSubConnMsg", properties: propertySet });
  console.log("ready")
  subscribeAllChannels(subscriberFileName);
});

function subscribeAllChannels(subscriberFileName) {
  try {
    const data = fs.readFileSync('./files/' + subscriberFileName + '.txt', 'utf-8');
    console.log(data);
    var subArray = data.split(',');
    // store a map of element,index
    // subArray.forEach(element => console.log(element))
    subArray.forEach(element => {
      channelList.add(element);
      executeAfterReady(element)
    })
  } catch (err) {
    console.error(err)
  }
}

sub.on("connect", function () {
  var propertySet = {
    errorMessage: "null",
    descriptiveMessage: "Redis Connection established"
  };
  client.trackEvent({ name: "redisSubConnMsg", properties: propertySet });
});

sub.on("error", (err) => {
  var propertySet = {
    errorMessage: "Something went wrong connecting redis",
    descriptiveMessage: err.message
  };
  client.trackEvent({ name: "redisSubConnError", properties: propertySet });
});

sub.on("close", function () {
  var propertySet = {
    errorMessage: "Redis server connection closed"
  };
  client.trackEvent({ name: "redisSubConnClosed", properties: propertySet });
  client.trackMetric({ name: "redisSubConnClosed", value: 1.0 });
});

function executeAfterReady(channelName) {
  sub.subscribe(channelName, (err, count) => {
    if (err) {
      var propertySet = {
        errorMessage: "couldn't subscribe to channel",
        descriptiveMessage: err.message,
        channelId: channelName,
      };
      client.trackEvent({ name: "redisSubConnError", properties: propertySet });
    } else {
      var propertySet = {
        errorMessage: "null",
        descriptiveMessage: "subscribed to channel",
        channelId: channelName
      };
      console.log("channel subscribed: " + channelName)
      channelToIndexMap.set(channelName, index);
      indexToChannelMap.set(index, channelName);
      index++;
      client.trackEvent({ name: "redisSubConn", properties: propertySet });
    }
  });
}


// var totalMessagesSent = new Array(constants.TOTAL_CHANNEL_PER_PUBLISHER).fill(0);
var totalMessageReceived = new Array(constants.TOTAL_CHANNELS_PER_CONNECTION).fill(0); // count of total messages received
var messageBatchReceived = new Array(constants.TOTAL_CHANNELS_PER_CONNECTION).fill(0);
var messageReceiveStarted = new Array(constants.TOTAL_CHANNELS_PER_CONNECTION).fill(false);
var lostMessages = new Array(constants.TOTAL_CHANNELS_PER_CONNECTION).fill(0);
var sequence = new Array(constants.TOTAL_CHANNELS_PER_CONNECTION).fill(-1);
var missingElementsMap = new Map();
var missingContentMap = new Map();
sub.on("message", (channel, message) => {
  processData(channel, message);
  // var messageObject = JSON.parse(message);
  // processMessage(messageObject);
  // totalMessageReceived++;
  // messageBatchReceived++;
  // messageReceiveStarted = true;
});

function processData(channel, message) {
  var messageObject = JSON.parse(message);
  // console.log("Channel: " + channel + " content: " + messageObject.content)
  var subIndex = channelToIndexMap.get(channel);
  processMessage(messageObject, subIndex);
  totalMessageReceived[subIndex]++;
  messageBatchReceived[subIndex]++;
  messageReceiveStarted[subIndex] = true;
}

function isNumberInSequence(content, subIndex) {
  if (content - sequence[subIndex] == 1) {
    return true;
  }
  return false;
}

function processMessage(messageObject, subIndex) {
  if (isNumberInSequence(messageObject.content, subIndex)) {
    var currentTime = Date.now();
    if (
      currentTime - messageObject.timestamp >
      constants.MESSAGE_EXPIRY_INTERVAL
    ) {
      lostMessages[subIndex]++;
    }
    sequence[subIndex]++;
  } else {
    if (messageObject.content < sequence[subIndex]) {
      // it is present in set
      var myMap = missingContentMap.get(subIndex);
      var mySet = missingElementsMap.get(subIndex);
      var storedMessage = myMap.get(messageObject.content);
      var currentTime = Date.now();
      if (
        currentTime - messageObject.timestamp >
        constants.MESSAGE_EXPIRY_INTERVAL
      ) {
        // currentTimestamp -
        lostMessages[subIndex]++;
      }
      mySet.delete(storedMessage);
      myMap.delete(storedMessage.content);
      missingElementsMap.set(subIndex, mySet);
      missingContentMap.set(subIndex, myMap);
    } else {
      console.log("out of sequence: " + messageObject.content + " sequence: " + sequence);
      var propertySet = {
        OutOfSequenceContent: messageObject.content,
        CurrentSequence: sequence[index],
        channelId: indexToChannelMap.get(index)
      };
      client.trackEvent({ name: "outOfSequence", properties: propertySet });
      for (var i = sequence[subIndex] + 1; i < messageObject.content; i++) {
        // add all missing elements in set and map
        var receivedMessage = new MessageReceived(i, messageObject.timestamp);
        var mySet = new Set();
        var myMap = new Map();
        if (missingElementsMap.has(subIndex)) {
          set = missingElementsMap.get(subIndex);
        }
        if (missingContentMap.has(subIndex)) {
          myMap = missingContentMap.get(subIndex);
        }
        mySet.add(receivedMessage);   // map of subIndex,set
        myMap.set(receivedMessage.content, receivedMessage);  // map of index,myMap
      }
      missingElementsMap.set(subIndex, mySet);
      missingContentMap.set(subIndex, myMap);
      sequence[subIndex] = messageObject.content; // update sequence
    }
  }
}


setInterval(sendMetricForAllChanels, constants.METRIC_SENT_INTERVAL);

function sendMetricForAllChanels() {
  channelList.forEach(channel => {
    var index = channelToIndexMap.get(channel);
    sendMetric(channel, index)
  })
}

function sendMetric(channelName, index) {
  if (messageReceiveStarted[index]) {
    processStoredElements(index);
    var propertySet = {
      totalMessageReceived: totalMessageReceived[index],
      lostMessages: lostMessages[index],
      messageBatchReceived: messageBatchReceived[index],
      channelId: channelName
    };
    var metrics = {
      lostMessages: lostMessages[index],
      MessageBatchReceived: messageBatchReceived[index],
    };
    // console.log("event: " + JSON.stringify(propertySet))
    // console.log("metrics: " + JSON.stringify(metrics))
    client.trackEvent({
      name: "subEvents",
      properties: propertySet,
      measurements: metrics,
    });
    resetValues(index);
  }
}

function processStoredElements(index) {
  var currentTime = Date.now();
  if (missingElementsMap.has(index)) {
    var mySet = missingElementsMap.get(index);
    var myMap = missingContentMap.get(index);
    mySet.forEach((item) => {
      if (currentTime - item.timestamp > constants.MESSAGE_EXPIRY_INTERVAL) {
        lostMessages[index]++;
        var messageSaved = myMap.get(item.content);
        mySet.delete(messageSaved);
        myMap.delete(item.content);
      }
    });
    missingElementsMap.set(index, mySet);
    missingContentMap.set(index, myMap);
  }
}

function resetValues(index) {
  lostMessages[index] = 0;
  messageBatchReceived[index] = 0;
}
