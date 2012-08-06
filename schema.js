var mongoose = require('mongoose'),
		Schema = mongoose.Schema,
		ObjectId = Schema.ObjectId;

var User = new Schema({});

mongoose.model('User', User);

module.exports = {
  models: ['User'], 
  expose: function(object, mongoose) {
    this.models.forEach(function(model) {
      object[model] = mongoose.model(model, this);
    });
  }
};
