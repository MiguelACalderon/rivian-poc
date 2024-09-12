import client from "./dataFederationMongoDB.js";

const query =  await client.db("rivian")
    .collection('notification')
    .find({ title: "Hello, World!"}).toArray();

console.log(query);