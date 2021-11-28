const mongodb = require("mongodb").MongoClient;
const logger = require("../logger")("dbOperations");

const url = process.env.MONGO_CONNECTION_STRING;

let db;

async function getDB() {
  if (!db) {
    const mongo = await mongodb.connect(url, {"useUnifiedTopology": true, "socketTimeoutMS": 5000});
    db = await mongo.db();
  }
  return db;
}

async function getCollection(collection) {
  return (await getDB()).collection(collection);
}

async function dropCollections(collections) {
  console.log("dropCollections called", collections);
  const dbCollections = (await (await getDB()).listCollections().toArray()).map(collection => collection.name);

  return Promise.all(
    collections
      .filter(collection => dbCollections.includes(collection))
      .map(async collection => (await getCollection(collection)).drop())
  );
}

async function findOne(collection, query, options) {
  console.log("findOne called", {collection, query});
  return (await getCollection(collection)).findOne(query, options);
}

async function find(collection, query = {}) {
  console.log("find called", {collection});
  return (await getCollection(collection)).find(query).project({"BLURN": 1, "_id": 0}).toArray();
}

async function findOneAndReplace(collection, filter, document) {
  console.log("upsertOne called", {collection, document});
  return (await getCollection(collection)).findOneAndReplace(filter, document, {"upsert": true});
}

async function createIndex(collection, index) {
  console.log("createIndex called", {collection});
  return (await getCollection(collection)).createIndex(index);
}

async function createIndexes(indexesData) {
  console.log("createIndexes called", indexesData);
  const promises = indexesData
    .map(({collectionName, indexes}) => Promise.all(indexes.map(index => createIndex(collectionName, index))));
  return Promise.all(promises);
}

async function aggregate(collection, query, options) {
  console.log("aggregate called", {collection, query});
  return (await getCollection(collection)).aggregate(query, {...options, "allowDiskUse": true}).toArray();
}

async function aggregateStream(collection, query, options) {
  console.log("aggregateStream called", {collection, query});
  return (await getCollection(collection)).aggregate(query, {...options, "allowDiskUse": true});
}

async function count(collection) {
  console.log("count called", {collection});
  return (await getCollection(collection)).estimatedDocumentCount();
}

async function updateMany(collection, filter, set) {
  console.log("updateMany called", {collection});
  return (await getCollection(collection)).updateMany(filter, set);
}

async function deleteMany(collection, query) {
  console.log("deleteMany called", {collection});
  return (await getCollection(collection)).deleteMany(query);
}

module.exports = {
  getDB,
  getCollection,
  dropCollections,
  find,
  findOne,
  findOneAndReplace,
  createIndexes,
  aggregate,
  aggregateStream,
  count,
  updateMany,
  deleteMany
};
