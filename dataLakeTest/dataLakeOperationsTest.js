const MongoClient = require("mongodb").MongoClient;
const mongoAtlasUrl = process.env.MONGO_ATLAS_CONNECTION_STRING;

// ------------------ To Use --------------------
// Please comment and uncomment the functions that you wish to make use of.
// * Please Note: If you wish to perform any .command operations on your Data Lake 
// you will require admin privileges for your user *
// -----------------------------------------------------

MongoClient.connect(mongoAtlasUrl, async function (err, client) {
  if (err) {
    console.error(err);
  }

  // Find query:
  // const collection = client.db().collection("theatersCollection");
  // const record = await collection.findOne({"location.address.street1": "1018 24th Ave SW"})
  // console.log(record, "------ record ---------")

  // List collections:
  const dbCollections = (await (client.db()).listCollections().toArray()) // .map(collection => collection.name);
  console.log(dbCollections, "------ collections ---------");

  // List Data Lake Stores:
  // const listStoresResult = await client.db().command({"listStores": 1});
  // console.log(listStoresResult.cursor.firstBatch, "--------------- listStoresResult --------------");

  // Create Data Lake Store:
  // const atlasStoreResult = await client.db().command({
  //   "createStore": "testStore",
  //   "provider": "atlas",
  //   "clusterName": "cluster0",
  //   "projectId": "5a7ae786df9db150b574fd75"
  // });
  // // console.log(atlasStoreResult, "--------------- atlasStoreResult --------------");

  // // Create Data Lake collection:
  // const atlasCollectionResult = await client.db().command({
  //   "create": "local-test",
  //   "dataSources": [
  //     {
  //       "collection": "movies",
  //       "database": "mflix",
  //       "storeName": "testStore"
  //     }
  //   ]
  // });
  // console.log(atlasCollectionResult, "--------------- atlasStoreResult --------------");

  // Drop Data Lake Store:
  // const dropStoreResult = await client.db().command({"dropStore": "Cluster0"});
  // console.log(dropStoreResult, "--------------- dropStoreResult --------------");
  
  // Drop Data Lake collection:
  // const collectionDropResult = await client.db().command({"drop": "usersCollection"});
  // console.log(collectionDropResult, "--------------- collectionDropResult --------------");

  // Drop Data Lake DB:
  // const dropDbResult = await client.db().command({"dropDatabase": 1});
  // console.log(dropDbResult, "--------------- dropDbResult --------------");

  // Data Lake: Output to s3:
  // const collection = client.db("dataLakeDb").collection("theatersCollection");
  // const records = await collection.aggregate([
  //   {"$limit": 10},
  //   {"$addFields": {"currentDate": new Date()}},
  //   {
  //     "$out": {
  //       "s3": {
  //         "bucket": "matt-atlas-test-bucket",
  //         "region": "eu-west-2",
  //         "filename": "theatersCollection-test4",
  //         "format": {"name": "parquet"}, // can set to a few different File formats
  //         "errorMode": "stop" // use errorMode setting for error handling scenarios
  //       }
  //     }
  //   }
  // ], {"allowDiskUse": true}).toArray();
  // console.log(records, "------------------ records output from data lake to s3 --------------");

  client.close();
});