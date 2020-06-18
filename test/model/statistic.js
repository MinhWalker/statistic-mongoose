const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const statisticSchema = new Schema({
    number: { type: Number, unique: true },
    createdOn: { type: Date, unique: true, 'default': Date.now }
});

module.exports = mongoose.model('statistic', statisticSchema);