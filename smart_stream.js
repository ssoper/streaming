var Stream = require('stream').Stream,
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    msgpack = require('msgpack-js');

function SmartStream() {
  var bufferSize = 16*1024*1024; // 16 Mb by default
  var currSize = 0;
  var buffer = [];
  var totalSize = 0;
  var totalCount = 0;
  var totalChunks = 0;

  function Public() {
    Stream.call(this);
    EventEmitter.call(this);
    this.writable = true;

    this.write = function(data) {
      if (data._doc) {
        currSize += msgpack.sizeof(data._doc);
        buffer.push(data._doc);
      }
      if (currSize > bufferSize) {
        this.flush();
      }
    };

    this.end = function(data) {
      if (data && data._doc) {
        currSize += msgpack.sizeof(data._doc);
        buffer.push(data._doc);
      }

      this.flush();
      this.close();
    };

    this.destroy = function() {
      this.flush();
      this.close();
    };

    this.flush = function() {
      totalCount += buffer.length;
      totalSize += currSize;
      totalChunks++;

      var flushed = buffer;
      this.emit('flush', flushed);
      buffer = [];
      currSize = 0;
    };

    this.close = function() {
      var self = this;
      process.nextTick(function() {
        self.emit('close', totalCount, totalSize, totalChunks);
      });
    };
  }

  util.inherits(Public, Stream);
  util.inherits(Public, EventEmitter);

  var object = new Public();

  Object.defineProperty(object, 'bufferSize', {
    get: function() { return bufferSize; },
    set: function(newSize) {
      bufferSize = newSize * 1024 * 1024;
    }
  });

  return object;
}

module.exports = new SmartStream();
