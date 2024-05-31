/**
 * @fileoverview various transforms for incoming data from the SDK into a format that can be used by the warehouse
 * basically we flatten stuff and clean up keys
 */

const dayjs = require('dayjs');
const { clone } = require('ak-tools');
const profileOps = ['$set', '$set_once', '$unset', '$delete', '$append', '$add', '$union', '$delete', '$increment'];

/** @typedef {import('../types').Runtimes} Runtimes */
/** @typedef {import('../types').Targets} Destinations */
/** @typedef {import('../types').Warehouse} Warehouse */
/** @typedef {import('../types').Lake} Lake */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TableNames */
/** @typedef {import('../types').Schema} Schema */

/** @typedef {import('../types').IncomingData} IncomingData */
/** @typedef {import('../types').FlatData} WarehouseData */
/** @typedef {import('../types').SchematizedData} SchematizedData */



/**
 * parses the incoming data from the SDK
 * @param  {string | Object | Object[]} reqBody
 * @returns {IncomingData}
 */
function parseSDKData(reqBody) {
	if (reqBody === undefined) return [];
	if (reqBody === null) return [];

	try {
		let data;

		if (typeof reqBody === 'string') {
			if (reqBody.startsWith("[") || reqBody.startsWith("{")) {
				if (reqBody.endsWith("]") || reqBody.endsWith("}")) {
					// handling JSON
					try {
						data = JSON.parse(reqBody);
					}
					catch (e) {
						// strangely not JSON
						throw new Error('unable to parse incoming data (tried JSON)');
					}
				}
			}

			// handling multipart form data
			else {
				try {
					data = JSON.parse(Buffer.from(reqBody, 'base64').toString('utf-8'));
				}
				catch (e) {
					// handling sendBeacon
					try {
						const body = reqBody.split("=").splice(-1).pop();
						if (!body) throw new Error('unable to parse incoming data (tried sendBeacon)');
						data = JSON.parse(Buffer.from(decodeURIComponent(body), 'base64').toString('utf-8'));
					}
					catch (e) {
						// we don't know what this is
						throw new Error('unable to parse incoming data (tried base64)');
					}

				}

			}
		}

		if (typeof reqBody === 'object') {
			if (Array.isArray(reqBody)) data = reqBody;
			if (!Array.isArray(reqBody)) data = [reqBody];
		}

		if (data && Array.isArray(data)) return data;
		if (data && !Array.isArray(data)) return [data];

		//should never get here
		throw new Error('unable to parse incoming data (unknown format)', reqBody);

	}
	catch (e) {
		console.error(e);
		console.error('unable to parse incoming data');
		console.error('reqBody:', reqBody);
		return [];
	}
}


/**
 * takes our flat object data and nests optional fields in a properties object
 * @param  {WarehouseData} data
 * @param  {Schema} schema
 * @returns {SchematizedData}
 */
function schematizeForWarehouse(data, schema) {
	if (!Array.isArray(data)) data = [data];
	const schematized = data.map(row => {
		const newRow = {};
		const now = new Date().toISOString();
		newRow["insert_time"] = now;
		for (const key in row) {
			const field = schema.find(f => f.name === key);
			if (field) {
				newRow[key] = row[key];
			} else {
				newRow.properties = newRow.properties || {};
				newRow.properties[key] = row[key];
			}
		}
		return newRow;
	});

	// @ts-ignore
	return schematized;
}


/**
 * prep data for each warehouse by basically flattening it + cleaning key names
 * also normalizing certain values
 * todo: allow a user to define their own values to schematize
 * @param  {IncomingData} data
 * @returns {WarehouseData}
 */
function flattenAndRenameForWarehouse(data) {
	const copy = clone(data);
	return copy.map(record => {
		//get rid of all $'s for /engage requests
		for (const key in record) {
			if (key.startsWith('$')) {
				// $set, $set_once, etc... are "operations" and their values are the "properties"
				if (profileOps.includes(key)) {
					record.operation = key;
					for (const prop in record[key]) {
						record[prop] = record[key][prop];
					}
					delete record[key];
				}

				//for $distinct_id, $token, $time, $ip, etc...
				else {
					record[key.slice(1)] = record[key];
					delete record[key];
				}
			}
			if (key === 'properties') {
				for (const prop in record.properties) {
					//get rid of all $'s in event properties
					if (prop.startsWith('$')) {
						record[prop.slice(1)] = record.properties[prop];
						delete record.properties[prop];
					}

					//convert time to ISO string... time might be sec or ms
					else if (prop === 'time') {
						const timeValue = record.properties.time;
						if (timeValue.toString().length === 13 && !timeValue?.toString()?.includes('.')) {
							// Unix timestamp in milliseconds
							record.event_time = dayjs(timeValue).toISOString();
						} else {
							// Unix timestamp in seconds
							record.event_time = dayjs.unix(timeValue).toISOString();
						}
						delete record.properties.time;
					}

					//todo: add more transformations here; perhaps a user-defined schema

					//if it's not a $, just move it up a level
					else {
						record[prop] = record.properties[prop];
						delete record.properties[prop];
					}

				}
				delete record.properties;  //side-note: do we have to keep deleting properties?
			}
		}
		return record;
	});
}

module.exports = { parseSDKData, schematizeForWarehouse, flattenAndRenameForWarehouse };