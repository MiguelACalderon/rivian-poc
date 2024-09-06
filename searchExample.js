/**
 * 
 * filter1 := bson.M{
  "parents.0.chilren.0.name": bson.M{
   "first_name":  "Mike",
   "last_name": "Anderson",
  },
}

 *  
 * 
 */

import client from "./mongodb.js";

const matchStage = { 
    $match: {
        template_processor: "PINPOINT",
        template_id: "pre-order-confirmation.json"
      }
};

const searchStage = {
    $search: {
        index: "default",
        text: {
          query: "test_metadata",
          path: {
            wildcard: "*"
          },
          fuzzy: {
            maxEdits: 2
          }, 
          
        },
        
    }    
};

const limit = { $limit: 5 };

const results = await client.db("rivian").collection("metadata").aggregate([searchStage, matchStage, limit]).toArray();

console.log(results);