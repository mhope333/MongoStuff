const MongoClient = require("mongodb").MongoClient;
const etl = require("etl");
const {sendSNS} = require("../awsOperations/sendMessages");
// * Please ensure you have your SNS and SQS configured correctly *

let mongoClient;
async function connect() {
  mongoClient = await MongoClient.connect("mongodb://localhost:27017/test");
  return mongoClient.db();
}

function raiseMessage(message) {
  console.log("------------- begin raising message ----------------");
  return sendSNS(message)
    .then(() => {
      console.log("------------- finished raising message ----------------");
    });
}

async function getMongoStream(db, collection) {
  return db.collection(collection).find({}).stream();
}

function getDataCollector() {
  return etl.collect(2);
}

function getDataTransformer() {
  return etl.map(records => {
    return records.map(record => {
      record.update = record.update ? false : true;
      console.log( "--------------- record updated ------------");
      return record;
    });
  });
}

function getMessageRaiser() {
  return etl.map(items => {
    return Promise.all(items.map(item => raiseMessage(item)));
  });
}

async function process() {
  const db = await connect();
  const mongoStream = await getMongoStream(db, "test");

  return mongoStream
    .pipe(getDataCollector())
    .pipe(getDataTransformer())
    // * Update mongo step here if you want *
    .pipe(getMessageRaiser())
    .promise()
    .then(() => console.log("--------------- finished processing ----------"))
    .catch(err => console.log(err, "------------ error in process -----------------"));
}

process();