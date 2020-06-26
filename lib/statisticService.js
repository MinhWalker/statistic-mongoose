const { Service } = require('feathers-mongoose');

const { filterQuery } = require('@feathersjs/commons');

const moment = require('moment');

class statisticService extends Service {

    isNum(n) {
        let result = /^[0-9,.]+$/.test(n);
        return result;
    }

    checkDate(date) {
        return moment(date, "YYYY-MM-DDTHH:mm:ss.sssZ").isValid()
    }

    convert_query(obj) {
        for (const property in obj) {
            for (var key in obj[property]) {
                if (obj[property].hasOwnProperty(key)) {
                    if (this.isNum(obj[property][key]) === true) {
                        obj[property][key] = parseFloat(obj[property][key]);
                    } else if (this.checkDate(obj[property][key]) === true) {
                        obj[property][key] = new Date(obj[property][key]);
                    } else {
                        obj[property][key] = obj[property][key];
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

        //* Add filters option

        // 1, $statistic
        var query_statistic = query.$statistic;

        if (query_statistic !== undefined) {

            filters.$statistic = parseInt(query.$statistic[0].value);
            //* Option for $statistic

            // 1, S_average
            // Field for create average value
            var average_field = "$" + query.$statistic[1].averageBy;
            var average_field_undolla = "" + query.$statistic[1].averageBy;

            // 2, S_sortBy
            // Dependent data field , default is createdAt
            var sort_field = "";
            var sort_field_undolla = "";
            if (query.$statistic[2].sortBy === undefined) {
                sort_field = "$createdAt";
                sort_field_undolla = "createdAt";
            } else {
                sort_field = "$" + query.$statistic[2].sortBy;
                sort_field_undolla = "" + query.$statistic[2].sortBy;
            }

            // 3, S_option
            // Option for create average by year, month ...
            var option_field = "";
            if (query.$statistic[3] === undefined) {
                option_field = "other";
            } else {
                option_field = query.$statistic[3].option;
            }

            // end task 
            delete query["$statistic"];
        }

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
        } else {

            const q = model.find(query).lean(this.lean);

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

}

module.exports = function init(options) {
    return new statisticService(options);
};

module.exports.statisticService = statisticService;
