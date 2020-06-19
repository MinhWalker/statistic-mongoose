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
        const average_field = "$" + query.averageBy;
        delete query["averageBy"];
        // Dependent data field , default is createdAt
        let sort_field = "";
        if (query.sortBy === undefined) {
            sort_field = "$createdAt";
        } else {
            sort_field = "$" + query.sortBy;
            delete query["sortBy"];
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

        super._find(params, count, getFilter = filterQuery);
    }


}

module.exports = function init(options) {
    return new statisticService(options);
};

module.exports.statisticService = statisticService;
