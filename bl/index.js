'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

const mongo = require("../mongo/client.js");

const async = require("async");

const token = require("./token.js");
const source = require("./source.js");
const destination = require("./destination.js");

const BLs = ["token", "source", "destination"];

let bl = {
	"token": token,
	"source": source,
	"destination": destination,
	
	"init": (options, cb) => {
		
		let fillModels = (blName, cb) => {
			if (!options[blName]) {
				return cb(new Error("Unable to find bl init options for: " + blName));
			}
			bl[blName].mongoClient = new mongo(options[blName]);
			bl[blName].mongoClient.init((err) => {
				return cb(err, blName);
			});
		};
		async.each(BLs, fillModels, function (err) {
			return cb(err);
		});
	}
};
module.exports = bl;