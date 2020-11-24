'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */
let bl = {
	"mongoClient": null,
	"_upsert": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					col.updateOne({"_id": opts.id}, {"$set": opts.body}, {"upsert": true}, (err, result) => {
						let response = null;
						if (result) {
							response = {
								"statusCode": 200,
								"result": result.result
							};
						}
						return cb(err, response);
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	},
	"_delete": (opts, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol(opts, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					col.deleteOne({"_id": opts.id}, {}, (err, result) => {
						let response = null;
						if (result) {
							response = {
								"statusCode": 200,
								"result": result.result
							};
						}
						return cb(err, response);
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	}
};

module.exports = bl;