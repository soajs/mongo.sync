'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

function Logger(options) {
	let self = this;
	self.debug = options.debug === "1";
}

Logger.prototype._debug = function () {
	let self = this;
	if (self.debug) {
		let args = Array.prototype.slice.call(arguments);
		args.unshift(new Date());
		console.debug(...args);
	}
};

Logger.prototype._info = function () {
	let args = Array.prototype.slice.call(arguments);
	args.unshift(new Date());
	console.info(...args);
};

Logger.prototype._error = function () {
	let args = Array.prototype.slice.call(arguments);
	args.unshift(new Date());
	console.error(...args);
};

module.exports = Logger;