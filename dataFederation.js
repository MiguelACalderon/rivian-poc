import client from "./dataFederationMongoDB.js";

const query =  await client.db("rivian").collection('notification').find({ title: "Hello, World!"}).toArray();




const outStage = {
    $out: {
        s3: {
            bucket: 'fabian-rsc',
            filename: "hello_rivian.json"
        }
    }
};

 

const out = await client.db("rivian").collection('notification').aggregate([outStage]);
// AFTER PUSHING FILES TO S3, WE CAN NOW INTEGRATE DATABRICKS

out.tryNext();

console.log(query);

