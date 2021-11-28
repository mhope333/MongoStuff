const getClient = require("mongodb-atlas-api-client");

const {dataLake, cloudProviderAccess} = getClient({
  "publicKey": "",
  "privateKey": "",
  "baseUrl": "https://cloud.mongodb.com/api/atlas/v1.0",
  "projectId": ""
});

const AWS = require("aws-sdk");
const iam = new AWS.IAM({
  "accessKeyId": "",
  "secretAccessKey": ""
});

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const assumeRolePolicy = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ""
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": ""
        }
      }
    }
  ]
};

const bucketAccessPolicy = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::demo-bucket-ashish",
        "arn:aws:s3:::demo-bucket-ashish/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::demo-bucket-ashish",
        "arn:aws:s3:::demo-bucket-ashish/*"
      ]
    }
  ]
};

async function deleteAll() {
  const {awsIamRoles} = await cloudProviderAccess.getAll();
  console.log(awsIamRoles);
  await Promise.all(awsIamRoles.map(role => cloudProviderAccess.delete("AWS", role.roleId)));
  console.log(await cloudProviderAccess.getAll());
}

const MongoClient = require("mongodb").MongoClient;
const uri = "mongodb://myuser:demo123@{HostName}/myFirstDatabase?ssl=true&authSource=admin";
(async () => {
  await deleteAll();
  // Create a cloud provider access
  const roleCreatedResult = await cloudProviderAccess.create({"providerName": "AWS"});
  console.log(roleCreatedResult);
  const roleId = roleCreatedResult.roleId;
  assumeRolePolicy.Statement[0].Principal.AWS = roleCreatedResult.atlasAWSAccountArn;
  assumeRolePolicy.Statement[0].Condition.StringEquals["sts:ExternalId"] = roleCreatedResult.atlasAssumedRoleExternalId;
  // Create IAM Role With Assumed Policy Document
  const iamRoleCreationResult = await iam.createRole({
    "AssumeRolePolicyDocument": JSON.stringify(assumeRolePolicy),
    "RoleName": "role-from-code"
  }).promise();
  console.log(iamRoleCreationResult);
  // Add Bucket Access Policy to existing role
  const iamRolePutResult = await iam.putRolePolicy({
    "PolicyDocument": JSON.stringify(bucketAccessPolicy),
    "RoleName": "role-from-code",
    "PolicyName": "S3-Bucket-Access-Policy"
  }).promise();
  console.log(iamRolePutResult);
  await wait(10000);
  // Set AWS Role Arn in Cloud Provider Access
  const awsArn = iamRoleCreationResult.Role.Arn;
  const roleUpdatedResult = await cloudProviderAccess.update(roleId, {"providerName": "AWS", "iamAssumedRoleArn": awsArn});
  console.log(roleUpdatedResult);
  // Create Data Lake
  const response = await dataLake.create({
    "name": "myFirstDataLake",
    // "cloudProviderConfig": {
      // "aws": {
      //   "roleId": roleId,
      //   "testS3Bucket": "matt-atlas-test-bucket"
      // }
    // }
  });
  console.log(response);
  const mongoUri = uri.replace("{HostName}", response.hostnames[0]);
  MongoClient.connect(mongoUri, async function (err, client) {
    if (err) {
      console.error(err);
    }
    // List Stores
    console.log(JSON.stringify(await client.db().command({
      "listStores": 1
    })));
    // // Create S3 Store
    // const s3StoreResult = await client.db().command({
    //   "createStore": "s3Store",
    //   "provider": "s3",
    //   "region": "eu-west-2",
    //   "bucket": "matt-atlas-test-bucket",
    //   "delimiter": "/"
    // });
    // console.log(JSON.stringify(s3StoreResult));
    // Create Atlas Store
    const atlasStoreResult = await client.db().command({
      "createStore": "atlasStore",
      "provider": "atlas",
      "clusterName": "Cluster0",
      "projectId": "5a7ae786df9db150b574fd75"
    });
    console.log(JSON.stringify(atlasStoreResult));
    // List Stores
    console.log(JSON.stringify(await client.db().command({
      "listStores": 1
    })));
    // Create Atlas Collection
    const atlasCollectionResult = await client.db().command({
      "create": "theatersCollection",
      "dataSources": [
        {
          "collection": "theaters",
          "database": "mflix",
          "storeName": "atlasStore"
        }
      ]
    });
    console.log(JSON.stringify(atlasCollectionResult));
    client.close();
  });
})();
