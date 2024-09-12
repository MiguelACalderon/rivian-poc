import fs from 'fs';
import csv from 'csv-parser';

// Import the MongoDB client
import client from './mongodb.js';


async function loadCSVtoMongoDB() {

  const database = client.db('rivian');
  const collection = database.collection('notification');

  // Helper function to parse JSON strings
  const parseJSON = (str) => {
    try {
      return JSON.parse(str);
    } catch (e) {
      return null;
    }
  };

  // Helper function to parse dates
  const parseDate = (dateStr) => {
    return dateStr ? new Date(dateStr) : null;
  };

  // Read the CSV file and insert data into MongoDB
  await new Promise((resolve, reject) => {
    fs.createReadStream('Table_2.csv')
      .pipe(csv())
      .on('data', async (row) => {
        try {
          // Transform the row data
          const transformedData = {
            _id: row.notification_id,
            created_at: parseDate(row.created_at),
            destination: row.destination,
            error_messages: parseJSON(row.error_messages),
            event_bus_events: parseJSON(row.event_bus_events),
            events: parseJSON(row.events),
            message: {
              message_id: row.message_id,
              message_name: row.message_name,
              message_type: row.message_type,
              provider: {
                provider_app_id: row.provider_app_id,
                provider_id: row.provider_id,
              },
            },
            rivian_id: row.rivian_id,
            status: row.status,
            updated_at: parseDate(row.updated_at),
          };

          // Insert transformed data into MongoDB
          await collection.insertOne(transformedData);
        } catch (error) {
          reject(error);
        }
      })
      .on('end', resolve)
      .on('error', reject);

  });

  // await client.close(); // Only close the client after all data is processed
  console.log('CSV file successfully processed and data loaded into MongoDB.');
}

loadCSVtoMongoDB().catch(console.error);
