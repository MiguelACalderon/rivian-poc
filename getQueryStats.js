import client from './mongodb.js';
import fs from "fs";
const query =  client.db("rivian").collection('metadata').find(
    {
      // Filters the documents where message_source is "T2D"
      message_source: "CS",

  
      // Filters documents where latest_version_for_type exists
      latest_version_for_type: { $exists: true }
    },
    {
      // Specifies to return all fields (return_fields: ["*"])
      projection: {}
    }
  )
  .sort({ 
    // Sorts the result by updated_at in descending order
    updated_at: -1 
  })
  .skip(0)
  .limit(500);  // Limits the result to 500 documents (size: 500)
  
   // Returns the query execution statistics
  const stats = await query.explain('executionStats');

  //write stats to a file
  fs.writeFileSync('statsBeforeIndex.json', JSON.stringify(stats.executionStats, 0 , 2));
  

  
/*
  DynamoDB query translated into MQL
  {
    "sort_fields": [
        {
            "updated_at": {
                "order": "desc"
            }
        }
    ],
    "return_fields": [
        "*"
    ],
    "size": 500,
    "offset": 0,
    "query": [
        {
            "name": "message_source",
            "values": [
                "T2D"
            ]
        },
        {
            "name": "metadata_info.activity_type",
            "values": [
                "vehicle_delivery"
            ]
        },
        {
            "name": "latest_version_for_type",
            "exists": true
        }
    ]
}

*/

export default query;