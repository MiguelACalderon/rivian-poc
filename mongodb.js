import { MongoClient } from 'mongodb';


// MongoDB connection URI
const uri = "mongodb+srv://test:123@demo.lfvxb.mongodb.net/?retryWrites=true&w=majority&appName=DEMO"

const client = new MongoClient(uri);
await client.connect(); // Ensure the client is connected

export default client;