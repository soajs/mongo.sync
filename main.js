'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

if (!process.env.SOAJS_MONGO_SYNC_OPTIONS) {
	throw new Error('You must set SOAJS_MONGO_SYNC_OPTIONS environment variable to point to an options.js file');
}

const options = require(process.env.SOAJS_MONGO_SYNC_OPTIONS);

const tryAfter = 10000;
const upsert = ["insert", "update", "replace"];
const Timestamp = require('mongodb').Timestamp;
const bl = require("./bl/index.js");

function get_opts(token, collection, cb) {
	let opts = {"colName": collection.s.colName, "dbName": collection.s.dbName, "stream": options.source.stream};
	if (token) {
		opts.token = token;
		return cb(null, opts);
	} else {
		bl.source.opsTime({"colName": "oplog.rs", "dbName": "local"}, (err, times) => {
			console.log(times);
			if (err) {
				console.debug("Unable to find oplog.rs time, using default time: " + options.firstOpTime);
				opts.firstOp = new Timestamp(1, new Date(options.firstOpTime).getTime() / 1000);
				return cb(null, opts);
			} else {
				opts.firstOp = times[0].ts;
				return cb(null, opts);
			}
		});
	}
}

function get_stream(collection, cb) {
	bl.token.get(collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err, token) => {
		if (err) {
			return cb(err);
		}
		get_opts(token, collection, (err, opts) => {
			if (err) {
				return cb(err);
			}
			bl.source._stream(opts, (err, stream) => {
				if (err) {
					return cb(err);
				}
				return cb(null, stream)
			});
		});
	});
}

function run_stream(collection, stream, cb) {
	stream.on("change", change => {
		stream.pause();
		if (change.operationType === "delete") {
			bl.destination._delete({
				"colName": collection.d.colName || change.ns.coll,
				"dbName": collection.d.dbName || change.ns.db,
				"id": change.documentKey._id
			}, (error, response) => {
				if (error) {
					console.error('Error:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					console.debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							console.error('Error:', err.message);
						}
					});
				}
			});
		} else if (upsert.includes(change.operationType)) {
			bl.destination._upsert({
				"colName": collection.d.colName || change.ns.coll,
				"dbName": collection.d.dbName || change.ns.db,
				"id": change.documentKey._id,
				"body": change.fullDocument
			}, (error, response) => {
				if (error) {
					console.error('Error:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					console.debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							console.error('Error:', err.message);
						}
					});
				}
			});
		} else {
			stream.resume();
			console.error("Unknown operationType", change.operationType)
		}
	});
	stream.on("error", error => {
		console.error('Error:', error.message);
		stream.close();
		return cb(null, "restart");
	});
}

function execute(collection) {
	get_stream(collection, (err, stream) => {
		if (err) {
			console.error('Error:', err.message);
			console.debug("Will try to execute again after " + tryAfter + " ms");
			setTimeout(() => {
				execute(collection);
			}, tryAfter);
		} else {
			run_stream(collection, stream, (err, action) => {
				if (err) {
					console.error('Error:', err.message);
				}
				if (action === "restart") {
					execute(collection);
				}
			});
		}
	});
}

if (options && options.collections) {
	bl.init(options, (err) => {
		if (err) {
			console.error('Error:', err.message);
		} else {
			for (let i = 0; i < options.collections.length; i++) {
				setImmediate(() => {
					execute(options.collections[i]);
				});
			}
		}
	});
} else {
	console.error("Missing collections under options!");
}