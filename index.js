var async = require('async');
var mongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017/ass3';

var chunkSize = parseInt(process.argv[2]);

var loadJob = (db, skip, limit, callback) => {
  var jobId = skip + '.' + limit;
  console.log('Starting job: ' + jobId);

  db.collection('data').find({}).skip(skip).limit(limit).toArray((error, accountData) => {
    if (error) {
      console.log(jobId + ' Error finding data...');
      callback();
      return;
    }
    db.collection('restored').find({}).skip(skip).limit(limit).toArray((error, restoredData) => {
      if (error) {
        console.log(jobId + ' Error finding restored data...');
        callback();
        return;
      }
      for (var index = 0; index < limit; index++) {
        Object.assign(accountData[index], restoredData[index]);
      }
      db.collection('okdata').insertMany(accountData, (error, result) => {
        if (error) {
          console.log(jobId + ' Error updating ' + accountData[index] + ":" + error);
          callback();
          return;
        }
        console.log(jobId + ' Job done');
        callback();
        return;
      });
    });
  });
}


mongoClient.connect(url, (error, dbo) => {
  if (error) {
    console.log('Error connecting to database');
    return -1;
  }
  var db = dbo.db('ass3');
  db.collection('data').count((error, numOfDocs) => {
    if (error) {
      console.log('Error counting data');
      dbo.close();
      return -1;
    }

    var numOfThreads = numOfDocs / chunkSize;
    var tasks = [];
    console.log (numOfThreads + '=' + numOfDocs + '/' + chunkSize);
    for (var i = 0; i < numOfThreads; i++) {
      const skip = i * chunkSize;
      tasks.push(function(callback) {
        loadJob(db, skip, chunkSize, callback);
      });
    }

    console.log('Start restoring data... ');

    async.parallel(tasks, () => {
      dbo.close();
      console.log('Disconnected');
    });
  });
});
