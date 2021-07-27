const dotenv = require("dotenv");
dotenv.config();
module.exports = {
  hostName: process.env.HOSTNAME,
  port: process.env.PORT,
  pwd: process.env.PASSWORD,
  appInsightKey: process.env.INSTRUMENTATION_KEY
};
