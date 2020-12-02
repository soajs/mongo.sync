'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */
const Mongo = require('mongodb');

let bl = {
	"mongoClient": null,
	
	"opsTime": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					col.find({}, {
						"projection": {"ts": 1, "ns": 1, "wall": 1},
						"sort": {$natural: 1},
						"limit": 1
					}, (err, cursor) => {
						if (err) {
							return cb(err);
						}
						return cursor.toArray(cb);
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	},
	"_stream": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					const pipeline = [
						{
							"$match": {
								"operationType": {
									"$in": opts.stream
								}
							}
						},
						{
							"$project": {
								"documentKey": true,
								"operationType": true,
								"ns": true,
								"fullDocument": true
							}
						}
					];
					let options = {"fullDocument": "updateLookup"};
					if (opts.token) {
						options.startAfter = opts.token;
					} else if (opts.time) {
						options.startAtOperationTime = opts.time;
					}
					let stream = col.watch(pipeline, options);
					return cb(null, stream);
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	},
	"_clone_count": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					let timestamp = new Date(opts.time);
					let hexSeconds = Math.floor(timestamp / 1000).toString(16);
					let _id_cutoff = Mongo.ObjectId(hexSeconds + "0000000000000000");
					let condition = {_id: {$lt: _id_cutoff}};
					col.countDocuments(condition, null, (err, count) => {
						return cb(err, count);
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	},
	
	"_clone": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					let timestamp = new Date(opts.time);
					let hexSeconds = Math.floor(timestamp / 1000).toString(16);
					let _id_cutoff = Mongo.ObjectId(hexSeconds + "0000000000000000");
					let condition = {_id: {$lt: _id_cutoff}};
					let stream = col.find(condition);
					return cb(null, stream);
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	}
};

module.exports = bl;