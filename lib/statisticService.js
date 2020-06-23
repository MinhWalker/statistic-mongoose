const { Service } = require('feathers-mongoose');

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
        const { filters, query } = getFilter(params.query || {});
        const discriminator = (params.query || {})[this.discriminatorKey] || this.discriminatorKey;
        const model = this.discriminators[discriminator] || this.Model;
        const q = model.find(query).lean(this.lean);

        // Add filters option 
        filters.$statistic = parseInt(query.$statistic);
        delete query["$statistic"];

        // Option for $statistic
        // Field for create average value
        const average_field = "$" + query.S_averageBy;
        delete query["S_averageBy"];
        // Dependent data field , default is createdAt
        let sort_field = "";
        if (query.S_sortBy === undefined) {
            sort_field = "$createdAt";
        } else {
            sort_field = "$" + query.S_sortBy;
            delete query["S_sortBy"];
        }

        // Handle $statistic
        if (filters.$statistic) {
            let aggregate_query = [
                // {$limit: undefined},
                { $match: this.convert_query(query) },
                {
                    $bucketAuto: {
                        groupBy: sort_field,
                        buckets: filters.$statistic,
                        output: {
                            "averagePrice": { $avg: average_field }
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

        // $select uses a specific find syntax, so it has to come first.
        if (Array.isArray(filters.$select)) {
            let fields = {};

            for (let key of filters.$select) {
                fields[key] = 1;
            }

            q.select(fields);
        } else if (typeof filters.$select === 'string' || typeof filters.$select === 'object') {
            q.select(filters.$select);
        }

        // Handle $sort
        if (filters.$sort) {
            q.sort(filters.$sort);
        }

        // Handle collation
        if (params.collation) {
            q.collation(params.collation);
        }

        // Handle $limit
        if (typeof filters.$limit !== 'undefined') {
            q.limit(filters.$limit);
        }

        // Handle $skip
        if (filters.$skip) {
            q.skip(filters.$skip);
        }

        // Handle $populate
        if (filters.$populate) {
            q.populate(filters.$populate);
        }

        let executeQuery = (total) => {
            return q.exec().then((data) => {
                return {
                    total,
                    limit: filters.$limit,
                    skip: filters.$skip || 0,
                    data
                };
            });
        };

        if (filters.$limit === 0) {
            executeQuery = (total) => {
                return Promise.resolve({
                    total,
                    limit: filters.$limit,
                    skip: filters.$skip || 0,
                    data: []
                });
            };
        }

        if (count) {
            return model.where(query)[this.useEstimatedDocumentCount ? 'estimatedDocumentCount' : 'countDocuments']()
                .exec()
                .then(executeQuery);
        }

        return executeQuery();

    }

}

module.exports = function init(options) {
    return new statisticService(options);
};

module.exports.statisticService = statisticService;
