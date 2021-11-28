const AWS = require("aws-sdk");
const sqs = new AWS.SQS({"apiVersion": "2012-11-05", region: 'eu-west-1'});
const sns = new AWS.SNS({apiVersion: '2010-03-31', region: 'eu-west-1'})

function sendBatch(items) {
  return new Promise((resolve, reject) => {
    const param = {
      "QueueUrl": process.env.AWS_SQS_URL,
      "Entries": items
    };

    return sqs.sendMessageBatch(param, (error, value) => {
      if (error) {
        return reject(error);
      }

      return resolve(value);
    });
  });
}

function sendSQS(payload) {
  console.log("----------------- sending to SQS --------------")
  const params = {
    "QueueUrl": process.env.AWS_SQS_URL,
    "MessageBody": JSON.stringify(payload)
  }
  return sqs.sendMessage(params).promise()
}

function sendSNS(payload) {
  console.log("----------------- sending to SNS --------------")
  const params = {
    "TopicArn": process.env.AWS_SNS_ARN,
    "Message": JSON.stringify(payload)
  }
  return sns.publish(params).promise();
}

module.exports = {sendSQS, sendSNS};