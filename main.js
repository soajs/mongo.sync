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

// 0 = turned off, 1 = get time from ops col, 2 = use time from options
let mongo_opsTime = process.env.SOAJS_MONGO_SYNC_OPSTIME || "0";
// 0 = turned off, 1 = turned on
let logger_debug = process.env.SOAJS_MONGO_SYNC_DEBUG || "0";

const options = require(process.env.SOAJS_MONGO_SYNC_OPTIONS);
const Logger = require("./lib/logger.js");
let _log = new Logger({"debug": logger_debug});

const tryAfter = 60000;
const upsert = ["insert", "update", "replace"];
const Timestamp = require('mongodb').Timestamp;
const bl = require("./bl/index.js");

function get_opts(token, collection, cb) {
	let opts = {"colName": collection.s.colName, "dbName": collection.s.dbName, "stream": options.source.stream};
	if (token) {
		opts.token = token;
		return cb(null, opts);
	} else {
		if (mongo_opsTime === "0") {
			return cb(null, opts);
		} else if (mongo_opsTime === "2") {
			opts.firstOp = new Timestamp(1, new Date(options.firstOpTime).getTime() / 1000);
			return cb(null, opts);
		}
		bl.source.opsTime({"colName": "oplog.rs", "dbName": "local"}, (err, times) => {
			if (err) {
				_log._debug("Unable to find oplog.rs time, using default time: " + options.firstOpTime);
				opts.firstOp = new Timestamp(1, new Date(options.firstOpTime).getTime() / 1000);
				return cb(null, opts);
			} else if (times) {
				opts.firstOp = times[0].ts;
				return cb(null, opts);
			} else {
				_log._debug("Unable to find oplog.rs time, skipping startAtOperationTime...");
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
			_log._debug("Starting sync .....");
			_log._debug(opts);
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
					_log._error('Error:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					_log._debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							_log._error('Error:', err.message);
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
					_log._error('Error:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					_log._debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							_log._error('Error:', err.message);
						}
					});
				}
			});
		} else {
			stream.resume();
			_log._error("Unknown operationType", change.operationType)
		}
	});
	stream.on("error", error => {
		_log._error('Error:', error.message);
		stream.close();
		return cb(null, "restart");
	});
}

function execute_sync(collection) {
	get_stream(collection, (err, stream) => {
		if (err) {
			_log._error('Error:', err.message);
			_log._debug("Will try to execute sync again after " + tryAfter + " ms");
			setTimeout(() => {
				execute_sync(collection);
			}, tryAfter);
		} else {
			run_stream(collection, stream, (err, action) => {
				if (err) {
					_log._error('Error:', err.message);
				}
				if (action === "restart") {
					execute_sync(collection);
				}
			});
		}
	});
}

function execute_copy(collection, cb) {
	bl.token.get(collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err, token) => {
		if (err) {
			_log._error('Error:', err.message);
			_log._debug("Will try to execute copy again after " + tryAfter + " ms");
			setTimeout(() => {
				execute_copy(collection, cb);
			}, tryAfter);
		} else {
			if (token) {
				return cb();
			} else {
				bl.source._clone({
					"date": options.firstOpTime,
					"colName": collection.s.colName,
					"dbName": collection.s.dbName
				}, (err, stream) => {
					if (err) {
						_log._error('Error:', err.message);
						_log._debug("Will try to execute copy again after " + tryAfter + " ms");
						setTimeout(() => {
							execute_copy(collection, cb);
						}, tryAfter);
					} else {
						_log._debug("Starting copy from: " + collection.s.dbName + " " + collection.s.colName, "To: " + collection.d.dbName + " " + collection.d.colName);
						stream.on("data", function (data) {
							stream.pause();
							bl.destination._upsert({
								"colName": collection.d.colName,
								"dbName": collection.d.dbName,
								"id": data._id,
								"body": data
							}, (err, response) => {
								if (err) {
									_log._error('Error:', err.message);
								}
								_log._debug("Copy doc with id [" + data._id + "]", "succeeded with status code", response.statusCode);
								stream.resume();
							});
						});
						stream.on("error", function (err) {
							stream.close();
							_log._error('Error:', err.message);
							return execute_copy(collection, cb);
						});
						
						stream.on("end", function () {
							_log._debug("Ending copy from: " + collection.s.dbName + " " + collection.s.colName, "To: " + collection.d.dbName + " " + collection.d.colName);
							return cb();
						});
					}
				});
			}
		}
	});
}

if (options && options.collections) {
	bl.init(options, (err) => {
		if (err) {
			_log._error('Error:', err.message);
		} else {
			for (let i = 0; i < options.collections.length; i++) {
				let collection = options.collections[i];
				if (collection.copy) {
					execute_copy(collection, () => {
						setImmediate(() => {
							execute_sync(collection);
						});
					});
				} else {
					setImmediate(() => {
						execute_sync(collection);
					});
				}
			}
		}
	});
} else {
	_log._error("Missing collections under options!");
}