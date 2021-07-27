const MongoClient = require("mongodb").MongoClient;

const clusterDbUrl = "";
// const dataLakeUrl = "";
const dataLakeUrl = "";

MongoClient.connect(dataLakeUrl, async function (err, client) {
  if (err) {
    console.error(err);
  }

  // standard DB / collection: 
  // const collection = client.db("mflix").collection("theaters");

  // data lake:
  const collection = client.db("dataLakeDb").collection("theatersCollection");

  const records = await collection.aggregate([
    {"$limit": 10},
    // {"$addFields": {"currentDate": new Date()}},
    {
      "$out": {
        "s3": {
          "bucket": "matt-atlas-test-bucket",
          "region": "eu-west-2",
          "filename": "theatersCollection-test4",
          // "filename": {"$toString": "$currentDate"},
          "format": {"name": "csv"},
          "errorMode": "stop" // use errorMode setting for error handling scenarios
        }
      }
    }
  ], {"allowDiskUse": true}).toArray();
  console.log(records);
  // perform actions on the collection object
  client.close();
});