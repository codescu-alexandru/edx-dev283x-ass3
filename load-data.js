var mongoClient = require('mongodb').MongoClient;
var data = require('./data/m3-customer-data.json');
var restored = require('./data/m3-customer-address-data.json');

const url = 'mongodb://localhost:27017/ass3';
const collectionData = 'data';
const collectionRestored = 'restored';

var loadData = (url, collection, jsonData) => {
  mongoClient.connect(url, (error, dbo) => {
    if (error) {
      console.log('Error connecting to database');
      return;
    }
    console.log('Connected to mongodb');
    var db = dbo.db('ass3');
    db.collection(collection).insertMany(jsonData, (error, result) => {
      if (error) {
        console.log('Error inserting into ' + collection);
      }
      dbo.close();
      console.log('Disconnected from mongodb');
    })
  });
};

loadData(url, collectionData, data);
loadData(url, collectionRestored, restored);
