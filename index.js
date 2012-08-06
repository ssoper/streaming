var async = require('async'),
    clusterFork = require('cluster-fork'),
    schema = require('./schema'),
    smartStream = require('./smart_stream'),
    readableFileSize = require('../lib/readable_file_size');

clusterFork.start(function(master) {
  var mongoose = require('mongoose');
  var models = {};
  mongoose.connect('mongodb://localhost/streaming')
  schema.expose(models, mongoose)

  var started = new Date();
  var numChunks = 0;
  var collated = {};
  var collate = function(mapped, cb) {
    Object.keys(mapped).forEach(function(key) {
      if (collated[key]) {
        collated[key] = collated[key].concat(mapped[key]);
      } else {
        collated[key] = mapped[key];
      }
    });
    cb();
  }

  smartStream.on('flush', function(users) {
    console.log(users.length + ' objects');
    master.nextWorker.send({
      map: true,
      data: users
    });
  }).on('close', function(count, size, chunks) {
    numChunks = chunks;
    console.log('Ingested ' + count + ' total objects, ' + readableFileSize(size) + ' total size');
  });

  var numMapped = 0;
  var numReduced = 0;
  master.on('processed', function(pid, msg) {
    if (msg.mapped) {
      console.log('Received a map with ' + Object.keys(msg.data).length + ' keys');
      collate(msg.data, function() {
        numMapped++;
        if (numMapped == numChunks) {
          var chunkSize = Math.ceil(Object.keys(collated).length/master.numWorkers);
          var chunkCount = 0;
          var collLength = Object.keys(collated).length;
          var chunk = {};
          console.log('Collating ' + collLength + ' keys');

          Object.keys(collated).forEach(function(key) {
            chunk[key] = collated[key];
            chunkCount++;

            if (Object.keys(chunk).length == chunkSize || chunkCount == collLength) {
              master.nextWorker.send({
                reduce: true,
                data: chunk
              });
              chunk = {};
            }
          });
        }
      });
    } else if (msg.reduced) {
      numReduced++;
      console.log('Received a reduce with ' + Object.keys(msg.data).length + ' keys');
      console.log(msg.data);
      if (numReduced == master.numWorkers) {
        var ended = new Date();
        console.log('Done, took ' + (ended-started)/1000 + ' seconds');
        process.exit();
      }
    }
  });

  models.User.find().limit().stream().pipe(smartStream);
}, function(worker, done) {

  var map = function() {
    var firstName = this.name.split(/\s/)[0];
    return [firstName, { count: 1 }];
  }

  var mapper = function(data, cb) {
    var mapped = {};
    data.forEach(function(datum) {
      var emitted = map.call(datum);
      var key = emitted[0];
      var value = emitted[1];

      if (mapped[key]) {
        mapped[key].push(value);
      } else {
        mapped[key] = [value];
      }
    });

    cb(mapped);
  }

  var reduce = function(key, values) {
    var result = { count: 0 };
    values.forEach(function(value) {
      result.count += value.count;
    });

    return result;
  };

  var reducer = function(mapped, cb) {
    var reduced = {};
    Object.keys(mapped).forEach(function(key) {
      reduced[key] = reduce.call(null, key, mapped[key]);
    });

    cb(reduced);
  }

  worker.on('message', function(msg) {
    if (msg.map) {
      mapper(msg.data, function(mapped) {
        done({ mapped: true, data: mapped });
      });
    } else if (msg.reduce) {
      reducer(msg.data, function(reduced) {
        done({ reduced: true, data: reduced });
      });
    }
  });
})