'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

const dbName = 'token';
const colName = 'tokens';

let bl = {
	"mongoClient": null,
	
	"get": (id, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol({"colName": colName, "dbName": dbName}, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					col.findOne({"_id": id}, (err, result) => {
						return cb(err, (result ? result.token : null));
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	},
	"save": (token, id, cb) => {
		if (bl.mongoClient) {
			bl.mongoClient.getCol({"colName": colName, "dbName": dbName}, (err, col) => {
				if (err) {
					return cb(err);
				} else {
					col.updateOne({"_id": id}, {
						"$set": {
							token,
							"lastModifiedDate": new Date()
						}
					}, {"upsert": true}, (err, result) => {
						return cb(err, (result ? result.result : null));
						
					});
				}
			});
		} else {
			return cb(new Error("Unable to find mongo client!"));
		}
	}
};

module.exports = bl;