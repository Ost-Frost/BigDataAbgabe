var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var CardsSchema = new Schema(
  {
    name: { type: String },
    id: { type: String },
    imageUrl: { type: String },
  },
  { versionKey: false }
);

mongoose.connect('mongodb://mongodb-container:27017/MTGCards');
var model = mongoose.model('mtgCards', CardsSchema, 'mtgCards');

async function getCards(name) {
  return await model.find({ name: new RegExp(name, 'i') }).exec();
}

module.exports = {
  getCards: getCards,
};
