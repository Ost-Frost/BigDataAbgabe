var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var CardsSchema = new Schema(
  {
    name: { type: String },
    id: { type: Schema.Types.Mixed },
    imageUrl: { type: String },
  },
  { versionKey: false }
);

mongoose.connect('mongodb://mongodb-container:27017/MTGCards');
var model = mongoose.model('mtgCards', CardsSchema, 'mtgCards');

async function getCards(name) {
  id = parseInt(name);
  if (!id || typeof id == NaN) {
    id = -1;
  }
  return await model.find({$or: [{ name: new RegExp(name, 'i') }, { id: id }]}).exec();
}

module.exports = {
  getCards: getCards,
};
