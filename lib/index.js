const hooks = require('./hooks');
const statisticService = require('./statisticService');

Object.assign(statisticService, { hooks, statisticService });

module.exports = statisticService;