# Statistic-mongoose

[![Build Status](https://travis-ci.org/feathersjs-ecosystem/feathers-mongoose.png?branch=master)](https://travis-ci.org/feathersjs-ecosystem/feathers-mongoose)

Override [Feathers](https://feathersjs.com) service adapter ([feathers-mongoose](https://github.com/feathersjs-ecosystem/feathers-mongoose)) for the [Mongoose](http://mongoosejs.com/) ORM.Additional querying syntax for create times series data . An object modeling tool for [MongoDB](https://www.mongodb.org/)

With npm
```bash
$ npm install --save statistic-mongoose
```

With yarn
```bash
$ yarn add statistic-mongoose
```

> __Important:__ `statistic-mongoose` implements the [Feathers Common database adapter API](https://docs.feathersjs.com/api/databases/common.html) and [querying syntax](https://docs.feathersjs.com/api/databases/querying.html).

> This adapter also requires a [running MongoDB](https://docs.mongodb.com/getting-started/shell/#) database server.


## SETUP

This version just support for ([feathers-mongoose](https://github.com/feathersjs-ecosystem/feathers-mongoose)) v6.3.0

You can refer to my `package.json`:

```json
"dependencies": {
    "@feathersjs/commons": "^1.3.0",
    "@feathersjs/errors": "^3.3.4",
    "lodash.omit": "^4.5.0",
    "uberproto": "^2.0.4"
},
"devDependencies": {
    "@feathersjs/express": "^1.2.7",
    "@feathersjs/feathers": "^3.2.3",
    "@feathersjs/socketio": "^3.2.7",
    "feathers-mongoose": "^6.3.0",
    "body-parser": "^1.18.3",
    "chai": "^4.2.0",
    "feathers-service-tests": "^0.10.2",
    "istanbul": "^1.1.0-alpha.1",
    "mocha": "^5.2.0",
    "mongoose": "^5.3.0",
    "semistandard": "^13.0.1",
    "sinon-chai": "^3.3.0",
    "sinon": "^7.1.1"
}
```

## API

### `service(options)`

Returns a new service instance initialized with the given options. `Model` has to be a Mongoose model. See the [Mongoose Guide](http://mongoosejs.com/docs/guide.html) for more information on defining your model.

```js
const mongoose = require('mongoose');
const createService = require('statistic-mongoose');

// A module that exports your Mongoose model
const Model = require('./models/message');

// Make Mongoose use the ES6 promise
mongoose.Promise = global.Promise;

// Connect to a local database called `feathers`
mongoose.connect('mongodb://localhost:27017/feathers');

app.use('/messages', createService({ Model }));
app.use('/messages', createService({ Model, lean, id, events, paginate }));
```

Read [feathers-mongoose](https://github.com/feathersjs-ecosystem/feathers-mongoose) docs for more. 

## Example

Here's a complete example of a Feathers server with a `statistic` Mongoose service, I'm using [@feathers/cli](https://github.com/feathersjs/cli) v3.2.0 for this example. 

In `statistic.model.js`: 

```js
module.exports = function (app) {
  const mongooseClient = app.get('mongooseClient');
  const { Schema } = mongooseClient;
  const statistic = new Schema({
    number: { type: Number, unique: true },
    createdOn: { type: Date, unique: true, 'default': Date.now }
  }, {
    timestamps: true
  });

  return mongooseClient.model('statistic', statistic);
```

I'm create 50 million documents for this example. You can see how to create a quick 50m record [here](https://vladmihalcea.com/mongodb-time-series-introducing-the-aggregation-framework/).

In `statistic.service.js`:

```js
const createService = require('statistic-mongoose');
const createModel = require('../../models/statistic.model');
const hooks = require('./statistic.hooks');

module.exports = function (app) {
  const Model = createModel(app);
  const paginate = app.get('paginate');

  const options = {
    name: 'statistic',
    Model,
    paginate
  };

  // Initialize our service with any options it requires
  app.use('/statistic', createService(options));
 
  // Get our initialized service so that we can register hooks and filters
  const service = app.service('statistic');

  service.hooks(hooks);
};
```

## Querying, Validation

You can see some basic Feathers querying [here](https://docs.feathersjs.com/api/databases/querying.html#equality). I added a few queries by override [feathers-mongoose](https://github.com/feathersjs-ecosystem/feathers-mongoose) module. 

## $statistic 

`$statistic` will return average value of the intervals

```js
//Get 100 average values following createdOn field
app.service('statistic').find({
  query: {
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?$statistic=100&S_averageBy=number&S_sortBy=createdOn`

__Options:__

- `S_averageBy` (**required**) - The field you need to statistical
- `S_sortBy` ((**required**)) - The field dependent values

> **Important:** `$statistic` will not support paging . You need to add fields S_averageBy and S_sortBy to access $statistic query. I use a general structure is S_ + field because if S_ is not present, it may be duplicated in database when performing record search. 

# Combine with other queries

## $limit 

`$limit` will Limited number of records 

```js
//Get 100 average values of 1000 records following createdOn field
app.service('statistic').find({
  query: {
    $limit: 1000,
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?$limit=1000&$statistic=100&S_averageBy=number&S_sortBy=createdOn`

## $skip 

`$skip` will skip values sort by query field 

```js
//Get 100 average values of createdOn field, skip 10 first record
app.service('statistic').find({
  query: {
    $skip: 10,
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?$skip=1000&$statistic=100&S_averageBy=number&S_sortBy=createdOn`

## $lt, $lte

Find all records where the value is less ($lt) or less and equal ($lte) to a given value for statistic analys.

```js
//Get 100 average values of values less then 10000
app.service('statistic').find({
  query: {
    number: {
      $lt: 10000
    }
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?number[$lt]=10000&$statistic=100&S_averageBy=number&S_sortBy=createdOn`

## $gt, $gte

Find all records where the value is more ($gt) or more and equal ($gte) to a given value.

```js
//Get 100 average values of values more then 10000
app.service('statistic').find({
  query: {
    number: {
      $gt: 10000
    }
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?number[$gt]=10000&$statistic=100&S_averageBy=number&S_sortBy=createdOn`

## $in, $nin

Find all records where the property does ($in) or does not ($nin) match any of the given values.

```js
//Find 100 average values in room 10 or 100000
app.service('statistic').find({
  query: {
    number: {
      $in: [ 10, 100000 ]
    }
    $statistic: 100,
    S_averageBy: number,
    S_sortBy: createdOn
  }
})
```

`GET /statistic?number[$in]=10&number[$in]=100000&$statistic=100&S_averageBy=number&S_sortBy=createdOn`


## License

[MIT](LICENSE)

## Authors

- [Minhwalker](https://github.com/MinhWalker)








