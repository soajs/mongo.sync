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

// 0 = turned off, 1 = start from yesterday, 2 = use time from options
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

function get_time(cb) {
	if (mongo_opsTime === "0") {
		return cb(null, null);
	} else if (mongo_opsTime === "1") {
		const today = new Date();
		const yesterday = new Date(today);
		yesterday.setDate(yesterday.getDate() - 1);
		let month = '' + (yesterday.getMonth() + 1);
		let day = '' + yesterday.getDate();
		let year = yesterday.getFullYear();
		if (month.length < 2) {
			month = '0' + month;
		}
		if (day.length < 2) {
			day = '0' + day;
		}
		return cb(null, [year, month, day].join('-'));
	} else if (mongo_opsTime === "2") {
		return cb(null, options.firstOpTime);
	}
}

function get_sync_stream(collection, cb) {
	bl.token.get(collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err, token) => {
		if (err) {
			return cb(err);
		}
		get_time((err, time) => {
			if (err) {
				return cb(err);
			}
			let opts = {
				"colName": collection.s.colName, "dbName": collection.s.dbName, "stream": options.source.stream
			};
			if (token) {
				opts.token = token;
			} else if (time) {
				opts.time = new Timestamp(1, new Date(time).getTime() / 1000);
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

function run_sync_stream(collection, stream, cb) {
	stream.on("change", change => {
		stream.pause();
		if (change.operationType === "delete") {
			bl.destination._delete({
				"colName": collection.d.colName || change.ns.coll,
				"dbName": collection.d.dbName || change.ns.db,
				"id": change.documentKey._id
			}, (error, response) => {
				if (error) {
					_log._error('Error sync:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					_log._debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							_log._error('Error sync:', err.message);
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
					_log._error('Error sync:', "Stream operationType [" + change.operationType + "] id [" + change.documentKey._id + "] failed with error: ", error.message);
					stream.close();
					return cb(null, "restart");
				} else {
					stream.resume();
					_log._debug("Stream operationType [" + change.operationType + "] with id [" + change.documentKey._id + "]", "succeeded with status code", response.statusCode);
					bl.token.save(change._id, collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err) => {
						if (err) {
							_log._error('Error sync:', err.message);
						}
					});
				}
			});
		} else {
			stream.resume();
			_log._debug("Unknown operationType", change.operationType)
		}
	});
	stream.on("error", error => {
		_log._error('Error sync:', error.message);
		stream.close();
		return cb(null, "restart");
	});
}

function execute_sync(collection) {
	get_sync_stream(collection, (err, stream) => {
		if (err) {
			_log._error('Error sync:', err.message);
			_log._info("Will try to execute sync again after " + tryAfter + " ms");
			setTimeout(() => {
				execute_sync(collection);
			}, tryAfter);
		} else {
			run_sync_stream(collection, stream, (err, action) => {
				if (err) {
					_log._error('Error sync:', err.message);
				}
				if (action === "restart") {
					execute_sync(collection);
				}
			});
		}
	});
}

function get_copy_stream(collection, cb) {
	bl.token.get(collection.s.dbName + "_" + collection.s.colName + "_TOKEN_ID", (err, token) => {
		if (err) {
			return cb(err, null);
		}
		if (token) {
			return cb(null, null);
		} else {
			get_time((err, time) => {
				if (err) {
					return cb(err);
				}
				let opts = {
					"colName": collection.s.colName, "dbName": collection.s.dbName
				};
				if (!time) {
					return cb(new Error("Cannot copy collection without ops time"));
				}
				opts.time = time;
				_log._debug("Starting copy from: " + collection.s.dbName + " " + collection.s.colName, "To: " + collection.d.dbName + " " + collection.d.colName);
				bl.source._clone_count(opts, (err, count) => {
					if (err) {
						return cb(err, null);
					}
					bl.source._clone(opts, (err, stream) => {
						if (err) {
							return cb(err, null);
						}
						return cb(null, stream, count)
					});
				});
			});
		}
	});
}

function run_copy_stream(collection, stream, count, cb) {
	let counter = 0;
	stream.on("data", function (data) {
		stream.pause();
		bl.destination._upsert({
			"colName": collection.d.colName,
			"dbName": collection.d.dbName,
			"id": data._id,
			"body": data
		}, (err, response) => {
			if (err) {
				_log._error('Error copy:', err.message);
				stream.close();
				return cb(null, "restart");
			}
			_log._debug("Copy doc with id [" + data._id + "]", "succeeded with status code", response.statusCode);
			stream.resume();
			++counter;
			if (count === counter) {
				_log._info("Copy success for " + collection.d.dbName + "." + collection.d.colName + ": " + count + " of " + counter);
			}
		});
	});
	stream.on("error", function (err) {
		_log._error('Error copy:', err.message);
		stream.close();
		return cb(null, "restart");
	});
	
	stream.on("end", function () {
		_log._debug("Ending copy from: " + collection.s.dbName + " " + collection.s.colName, "To: " + collection.d.dbName + " " + collection.d.colName);
		return cb();
	});
}

function execute_copy(collection, cb) {
	get_copy_stream(collection, (err, stream, count) => {
		if (err) {
			_log._error('Error copy:', err.message);
			_log._info("Will try to execute copy again after " + tryAfter + " ms");
			setTimeout(() => {
				execute_copy(collection, cb);
			}, tryAfter);
		} else {
			if (!stream) {
				return cb();
			}
			run_copy_stream(collection, stream, count, (err, action) => {
				if (err) {
					_log._error('Error copy:', err.message);
				}
				if (action === "restart") {
					execute_copy(collection);
				} else {
					return cb();
				}
			});
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