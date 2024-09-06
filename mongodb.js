import { MongoClient } from 'mongodb';


// MongoDB connection URI
const uri = "CONNECTION"

const client = new MongoClient(uri);
await client.connect(); // Ensure the client is connected

export default client;