module.exports = Object.freeze({
  NUM_OF_MESSAGES: 5, // total number of messages published in a single go
  MESSAGE_PUBLISH_INTERVAL: 200, // Messages publishing interval i.e. every x milliseconds messages will be published to redis
  METRIC_SENT_INTERVAL: 60000, // ideal to be 1 minute
  MESSAGE_EXPIRY_INTERVAL: 300000,
  TOTAL_CHANNELS_PER_CONNECTION: 75
});
