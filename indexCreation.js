import client from "./mongodb.js";
import query from "./getQueryStats.js";
import fs from "fs";
try {
    console.log("Creating index for metadata collection");

    await client.db("rivian").collection("metadata").createIndex(
        {
        message_source: 1, // 1 for ascending order
        latest_version_for_type: 1, // 1 for ascending order
        updated_at: -1 // -1 for descending order (since the query sorts by updated_at in descending order)
        }
    );
    
    console.log("Index created successfully");
    console.log("Running query with the new index");
    const stats = await query.explain("executionStats");

    fs.writeFileSync('statsWithIndex.json', JSON.stringify(stats.executionStats, 0 , 2));
} catch (error) {
    console.error("Error creating index:", error.message);
}


  