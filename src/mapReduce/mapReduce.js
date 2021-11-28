// written for mongo version "3.4.1"
const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");
const testDocs = require("./test.json");

// Connection URL
const url = "mongodb://localhost:27017";
// Database Name
const dbName = "testDB";
// Create a new MongoClient
const client = new MongoClient(url);

// ------------------------------------------------------
function insertDocs(collection, documents) {
  collection.insertMany(documents);
}

function mapFunction1() {
  emit(this.cust_id, this.price); // eslint-disable-line
}

function reduceFunction1(keyCustId, valuesPrices) {
  return Array.sum(valuesPrices);
}

const query  = {
  "privacy.privateData": true
  // "privacy": { 
  //   "privateData": true
  // }
};

// Put test.json into mongo testDB - testCollection before running this file.
// Perform the map-reduce operation on all docs in testCollection1 to group by the cust_id, 
// and calculate the sum of the price for each cust_id:
// This operation can either output the results to a collection. OR return results directly.

// Use connect method to connect to the Server
client.connect(async (err) => {
  assert.equal(null, err);
  console.log("Connected successfully to server");

  const db = client.db(dbName);
  const collection = db.collection("testCollection1");

  await collection.remove({}); // clear any data in collection before running below.

  await insertDocs(collection, testDocs); // insert test data

  console.log("running mapReduce");
  const result = await collection.mapReduce(
    mapFunction1,
    reduceFunction1,
    // {out: "map_reduce_example"} // outputs into newCollection
    { // options 
      out: { inline: 1 }, // returns result 
      // can pass a mongo query (basically a filter) so that only docs returned by this query are operated on.
      "query": query
    } 
  );
  console.log("completed mapReduce operation", result);

  client.close();
});