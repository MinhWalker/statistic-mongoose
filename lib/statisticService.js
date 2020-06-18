const Service = require('feathers-mongoose');

const { filterQuery } = require('@feathersjs/commons');

class statisticService extends Service {
    constructor(options) {
        super(options);
        if (!options) {
            throw new Error('Mongoose options have to be provided');
        }

        if (!options.Model || !options.Model.modelName) {
            throw new Error('You must provide a Mongoose Model');
        }

        this.Model = options.Model;
        this.discriminatorKey = this.Model.schema.options.discriminatorKey;
        this.discriminators = {};
        (options.discriminators || []).forEach((element) => {
            if (element.modelName) {
                this.discriminators[element.modelName] = element;
            }
        });
        this.id = options.id || '_id';
        this.paginate = options.paginate || {};
        this.lean = options.lean === undefined ? true : options.lean;
        this.overwrite = options.overwrite !== false;
        this.events = options.events || [];
        this.useEstimatedDocumentCount = !!options.useEstimatedDocumentCount;
    }

    extend(obj) {
        super.extend(obj);
    }

    convert_query(obj) {
        for (var key in obj[Object.keys(obj)]) {
            if (obj[Object.keys(obj)].hasOwnProperty(key)) {
                obj[Object.keys(obj)][key] = parseInt(obj[Object.keys(obj)][key]);
            }
        }
        return obj;
    }

    // Override FIND method 
    _find(params, count, getFilter = filterQuery) {
        super._find(params, count, getFilter = filterQuery);

        // Add filters option 
        filters.$statistic = parseInt(query.$statistic);
        delete query["$statistic"];

        // Handle $statistic
        if (filters.$statistic) {
            let aggregate_query = [
                // {$limit: undefined},
                { $match: this.convert_query(query) },
                {
                    $bucketAuto: {
                        groupBy: "$createdOn",
                        buckets: filters.$statistic,
                        output: {
                            "averagePrice": { $avg: "$number" }
                        }
                    }
                }
            ]

            if (params.query.hasOwnProperty('$limit')) {
                let term = parseInt(params.query.$limit);
                if (aggregate_query.length !== 1) {
                    aggregate_query.splice(1, 0, { $limit: term });
                } else {
                    aggregate_query.unshift({ $limit: term });
                }
            }

            return model.aggregate(aggregate_query).allowDiskUse(true).then(result => {
                return result;
            })
        }
    }

    find(params) {
        super.find(params);
    }

    // Override GET method
    _get(id, params = {}, getFilter = filterQuery) {
        super._get(id, params = {}, getFilter = filterQuery);
    }

    get(id, params) {
        super.get(id, params);
    }

    _getOrFind(id, params) {
        super._getOrFind(id, params);
    }

    // Override CREATE method 
    create(data, params) {
        super.create(data, params);
    }

    // Override UPDATE method 
    update(id, data, params) {
        super.update(id, data, params);
    }

    // Override PATCH method 
    patch(id, data, params) {
        super.patch(id, data, params);
    }

    // Override REMOVE method 
    remove(id, params) {
        super.remove(id, params);
    }

}

module.exports = function init(options) {
    return new statisticService(options);
};

module.exports.statisticService = statisticService;
