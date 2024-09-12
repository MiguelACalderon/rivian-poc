import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';    

dotenv.config(); // Load environment variables from .env file

// MongoDB connection URI
const uri = process.env.MONGODB_DF;

const client = new MongoClient(uri);
await client.connect(); // Ensure the client is connected

export default client;