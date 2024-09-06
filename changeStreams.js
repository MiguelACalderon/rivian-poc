import client from "./mongodb.js";


const collection = client.db("rivian").collection('metadata');

const changeStream = collection.watch();


const changeStreamListen = () => {
        changeStream.on('change', next => {
        // process next document
            console.log('New event', next.operationType);
        });
}

changeStreamListen();