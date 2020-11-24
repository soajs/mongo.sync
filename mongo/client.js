'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

const MongoClient = require('mongodb').MongoClient;

function Client(options) {
	let __self = this;
	
	__self.uri = options.uri;
	
	__self.client = null;
}

Client.prototype.init = function (cb) {
	let __self = this;
	
	MongoClient.connect(__self.uri, (err, client) => {
		if (client) {
			__self.client = client;
			return cb(null);
		} else {
			return cb(err);
		}
	});
};

Client.prototype.getCol = function (options, cb) {
	let __self = this;
	
	__self.client.db(options.dbName).collection(options.colName, (err, col) => {
		return cb(err, col);
	});
};

Client.prototype.getDb = function (options, cb) {
	let __self = this;
	
	let db = __self.client.db(options.dbName);
	return cb(null, db);
};
module.exports = Client;