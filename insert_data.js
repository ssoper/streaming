var faker = require('Faker'),
    mongoose = require('mongoose'),
    async = require('async'),
    schema = require('./schema');
//     msgpackJs = require('msgpack-js'),

mongoose.connect('mongodb://localhost/streaming')
var models = {};
schema.expose(models, mongoose)

var insertData = function() {
  var i = 0;
  async.until(function() {
    return i == 1000000;
  }, function(cb) {
    var user = new models.User(faker.Helpers.userCard());
    user.save(function(err, updated) {
      i++;
      cb();
    });    
  }, function(err) {
    err && console.log(err) && process.exit(1);
    console.log(i + ' users inserted');
    process.exit(0);
  });
}

insertData();
