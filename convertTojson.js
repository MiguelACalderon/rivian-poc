import fs from 'fs';
import csv from 'csv-parser';

// Import the MongoDB client
import client from './mongodb.js';


async function loadCSVtoMongoDB() {

  const database = client.db('rivian');
  const collection = database.collection('metadata');

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
    fs.createReadStream('sample.csv')
      .pipe(csv())
      .on('data', async (row) => {
        try {
          // Transform the row data
          const transformedData = {
            metadata_id: row.metadata_id,
            consolidation_info: parseJSON(row.consolidation_info),
            created_at: parseDate(row.created_at),
            latest_version_for_type: row.latest_version_for_type,
            message_name: row.message_name,
            message_source: row.message_source,
            metadata_info: parseJSON(row.metadata_info),
            mobile_app_name: row.mobile_app_name,
            notes: row.notes,
            notification_type: row.notification_type,
            notification_type_template_version: row.notification_type_template_version,
            pinpoint_app_name: row.pinpoint_app_name,
            preference_ids: parseJSON(row.preference_ids),
            recipient_group: row.recipient_group,
            recipient_type: parseJSON(row.recipient_type),
            recipient_type_attribute: parseJSON(row.recipient_type_attribute),
            template_id: row.template_id,
            template_processor: row.template_processor,
            template_provider: row.template_provider,
            template_version: parseInt(row.template_version, 10),
            transform_event_mapping: parseJSON(row.transform_event_mapping),
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
