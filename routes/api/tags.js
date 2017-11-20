var router = require('express').Router();
var mongoose = require('mongoose');
var Article = mongoose.model('Article');

// return a list of tags
router.get('/', function(req, res, next) {
  Article.aggregate([
    { "$unwind" : "$tagList" },
    {"$group": {_id:"$tagList", count:{$sum:1}}},
    {"$sort":{"count":-1}}], function(err,tags){
      return res.json({tags: tags.map(value=> value._id)});
    });
  // Article.find().distinct('tagList').then(function(tags){
  //   return res.json({tags: tags});
  // }).catch(next);
});

module.exports = router;
