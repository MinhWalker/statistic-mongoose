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

    isPositiveInteger(n) {
        return n >>> 0 === parseFloat(n);
    }

    convert_query(obj) {
        for (const property in obj) {
            for (var key in obj[property]) {
                if (obj[property].hasOwnProperty(key)) {
                    if (this.isPositiveInteger(obj[property][key]) === true) {
                        obj[property][key] = parseInt(obj[property][key]);
                    } else {
                        obj[property][key] = new Date(obj[property][key]);

                    }

                }
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

        console.log({ filters, query });

        //* Add filters option

        // 1, $statistic
        filters.$statistic = parseInt(query.$statistic);
        delete query["$statistic"];

        //* Option for $statistic

        // 1, S_average
        // Field for create average value
        var average_field = "$" + query.S_averageBy;
        var average_field_undolla = "" + query.S_averageBy;
        delete query["S_averageBy"];

        // 2, S_sortBy
        // Dependent data field , default is createdAt
        var sort_field = "";
        var sort_field_undolla = "";
        if (query.S_sortBy === undefined) {
            sort_field = "$createdAt";
            sort_field_undolla = "createdAt";
        } else {
            sort_field = "$" + query.S_sortBy;
            sort_field_undolla = "" + query.S_sortBy;
            delete query["S_sortBy"];
        }

        // 3, S_option
        // Option for create average by year, month ...
        var option_field = "";
        if (query.S_option === undefined) {
            option_field = "other";
        } else {
            option_field = query.S_option;
            delete query["S_option"];
        }

        // 4. S_dateMax, S_dateMin      // dateMin = your_field[$gte] , dateMin = your_field[$gte]
        // Option for create average by year, month ...
        // if (option_field !== "other") {
        //     var dateMax = '';
        //     var dateMin = '';

        //     let dateMax_query = parseInt(query.S_dateMax);
        //     let dateMin_query = parseInt(query.S_dateMin);

        //     if (query.S_dateMax === undefined) {
        //         throw new Error('Invalid S_dateMax field');
        //     } else if (query.S_dateMin === undefined) {
        //         throw new Error('Invalid S_dateMin field');
        //     } else if (query.S_dateMax === undefined && query.S_dateMin === undefined) {
        //         throw new Error('Invalid S_dateMin and S_dateMax field');
        //     } else {
        //         if (dateMax_query < 10000 && dateMin_query < 10000) {
        //             dateMax = query.S_dateMax;
        //             dateMin = query.S_dateMin;
        //         } else {
        //             let term_max = new Date(dateMax_query);
        //             let term_min = new Date(dateMin_query);
        //             dateMax = term_max.toISOString();
        //             dateMin = term_min.toISOString();
        //         }
        //         delete query["S_dateMax"];
        //         delete query["S_dateMin"];
        //     }
        // }

        // Handle $statistic
        if (filters.$statistic) {
            let aggregate_query = [];

            switch (option_field) {
                case "year":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1
                            }
                        }
                    ];
                    break;
                case "month":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1
                            }
                        }
                    ];
                    break;
                case "day":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: sort_field
                                    },
                                    "dayOfMonth": {
                                        $dayOfMonth: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.dayOfMonth": 1
                            }
                        }
                    ];
                    break;
                case "hour":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: sort_field
                                    },
                                    "dayOfMonth": {
                                        $dayOfMonth: sort_field
                                    },
                                    "hour": {
                                        $hour: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.dayOfMonth": 1,
                                "_id.hour": 1
                            }
                        }
                    ];
                    break;
                case "minute":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: sort_field
                                    },
                                    "dayOfMonth": {
                                        $dayOfMonth: sort_field
                                    },
                                    "hour": {
                                        $hour: sort_field
                                    },
                                    "minute": {
                                        $minute: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.dayOfMonth": 1,
                                "_id.hour": 1,
                                "_id.minute": 1
                            }
                        }
                    ];
                    break;
                case "second":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: sort_field
                                    },
                                    "dayOfMonth": {
                                        $dayOfMonth: sort_field
                                    },
                                    "hour": {
                                        $hour: sort_field
                                    },
                                    "minute": {
                                        $minute: sort_field
                                    },
                                    "second": {
                                        $second: sort_field
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.dayOfMonth": 1,
                                "_id.hour": 1,
                                "_id.minute": 1,
                                "_id.second": 1
                            }
                        }
                    ];
                    break;
                case "millisecond":
                    aggregate_query = [
                        {
                            $match: this.convert_query(query)
                        },
                        {
                            $group: {
                                "_id": {
                                    "year": {
                                        $year: sort_field
                                    },
                                    "month": {
                                        $month: "$createdOn"
                                    },
                                    "dayOfMonth": {
                                        $dayOfMonth: "$createdOn"
                                    },
                                    "hour": {
                                        $hour: "$createdOn"
                                    },
                                    "minute": {
                                        $minute: "$createdOn"
                                    },
                                    "second": {
                                        $second: "$createdOn"
                                    },
                                    "millisecond": {
                                        $millisecond: "createdOn"
                                    }
                                },
                                "avg": {
                                    $avg: average_field
                                }
                            }
                        },
                        {
                            $sort: {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.dayOfMonth": 1,
                                "_id.hour": 1,
                                "_id.minute": 1,
                                "_id.second": 1,
                                "_id.millisecond": 1
                            }
                        }
                    ];
                    break;
                case "other":
                    aggregate_query = [
                        // {$limit: undefined},
                        { $match: this.convert_query(query) },
                        {
                            $bucketAuto: {
                                groupBy: sort_field,
                                buckets: filters.$statistic,
                                output: {
                                    "average_value": { $avg: average_field }
                                }
                            }
                        }
                    ];
                    break;
                default:
                    throw new Error('Invalid option');
            }

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
