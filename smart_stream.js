var Stream = require('stream').Stream,
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    msgpack = require('msgpack-js');

function SmartStream() {
  Stream.call(this);
  EventEmitter.call(this);

  var maxSize = 16*1024*1024; // 16 Mb
  var bufferSize = 0;
  var buffer = [];
  var totalSize = 0;
  var totalCount = 0;
  var totalChunks = 0;

  this.writable = true;

  this.write = function(data) {
    if (data._doc) {
      bufferSize += msgpack.sizeof(data._doc);
      buffer.push(data._doc);
    }
    if (bufferSize > maxSize) {
      this.flush();
    }
  };

  this.end = function(data) {
    if (data && data._doc) {
      bufferSize += msgpack.sizeof(data._doc);
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
    totalSize += bufferSize;
    totalChunks++;

    var flushed = buffer;
    this.emit('flush', flushed);
    buffer = [];
    bufferSize = 0;
  };

  this.close = function() {
    var self = this;
    process.nextTick(function() {
      self.emit('close', totalCount, totalSize, totalChunks);
    });
  }
}

util.inherits(SmartStream, Stream);
util.inherits(SmartStream, EventEmitter);

module.exports = new SmartStream();
