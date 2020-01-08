const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");
// const testDoc = require("./test.json");

// Connection URL
const url = "mongodb://localhost:27017";
// Database Name
const dbName = "testDB";
// Create a new MongoClient
const client = new MongoClient(url);


function mapFunction1() {
  emit(this.cust_id, this.price); // eslint-disable-line
}

function reduceFunction1(keyCustId, valuesPrices) {
  return Array.sum(valuesPrices);
}

// Put test.json into mongo testDB - testCollection before running this file.
// Perform the map-reduce operation on the testCollection1 to group by the cust_id, 
// and calculate the sum of the price for each cust_id:
// This operation outputs the results to a collection named map_reduce_example.

// Use connect method to connect to the Server
client.connect(async (err) => {
  assert.equal(null, err);
  console.log("Connected successfully to server");

  const db = client.db(dbName);
  const collection = db.collection("testCollection1");

  console.log("running mapReduce");
  const result = await collection.mapReduce(
    mapFunction1,
    reduceFunction1,
    {out: "map_reduce_example"}
  );
  console.log("completed mapReduce operation");

  client.close();
});

// insertDoc(db) {
// }


