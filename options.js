'use strict';

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

module.exports = {
	"firstOpTime": "2020-10-27",
	"token": {
		"uri": "mongodb://localhost:27017?useUnifiedTopology=true"
	},
	"source": {
		"uri": "mongodb+srv://USERNAME:PASSWORD@CLUSTER.mongodb.net?retryWrites=true&w=majority&useUnifiedTopology=true",
		"stream": ["insert", "update", "replace", "delete"]
	},
	"destination": {
		"uri": "mongodb://localhost:27017?useUnifiedTopology=true"
	},
	"collections": [
		{
			"copy": true, // if first sync copy all date prior to firstOpTime then turn on sync from that date
			"s": {
				"dbName": "core_provision",
				"colName": "custom_registry"
			},
			"d": {
				"dbName": "new_db",
				"colName": "custom_registry"
			}
		}
	]
};