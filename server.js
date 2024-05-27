// A row-level Mixpanel proxy server which receives data from Mixpanel's JS Lib and sends it to DWHs.
// by ak@mixpanel.com


// TYPES
/** @typedef {import('./types').Runtimes} Runtimes */
/** @typedef {import('./types').Destinations} Destinations */
/** @typedef {import('./types').Endpoints} Endpoints */
/** @typedef {import('./types').IncomingData} IncomingData */
/** @typedef {import('./types').FlatData} WarehouseData */

// DEPENDENCIES
const express = require('express');
const app = express();
const { version } = require('./package.json');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);


// CODE SPLITTING
const setupCORS = require('./components/corsConfig');
const proxyAssets = require('./components/proxyAssets');
const validateEnv = require('./components/validate');
const bodyParse = require('./components/bodyParse');
const { parseSDKData } = require('./components/parser');
const log = require('./components/logger');

// MIDDLEWARE
const bigquery = require('./middleware/bigquery');
const snowflake = require('./middleware/snowflake');
const redshift = require('./middleware/redshift');
const mixpanel = require('./middleware/mixpanel');
const middleware = { bigquery, snowflake, redshift, mixpanel };

// HELPERS
const { clone } = require('ak-tools');
const profileOps = ['$set', '$set_once', '$unset', '$delete', '$append', '$union', '$delete', '$increment'];


// ENV
require('dotenv').config();
const PARAMS = validateEnv();
const NODE_ENV = process.env.NODE_ENV || 'prod';
if (NODE_ENV === 'dev') { log.verbose(true); log.cli(true); }
if (NODE_ENV === 'test') { log.verbose(true); log.cli(true); }
if (NODE_ENV === 'prod') { log.verbose(false); log.cli(false); }
log(`running in ${NODE_ENV} mode; version: ${version}; verbose: ${log.isVerbose()} cli: ${log.isCli()}`);
const PORT = process.env.PORT || 8080;
let FRONTEND_URL = process.env.FRONTEND_URL || "";
if (FRONTEND_URL === "none") FRONTEND_URL = "";

// WAREHOUSES + DESTINATIONS
const WAREHOUSES = process.env.WAREHOUSES || "MIXPANEL";
/** @type {Destinations[]} */
const DESTINATIONS = WAREHOUSES.split(',').map(wh => wh.trim()).filter(a => a).map(t => t.toLowerCase());
/** @type {Runtimes} */
let RUNTIME = process.env.RUNTIME?.toUpperCase() || 'LOCAL';
const EVENTS_TABLE_NAME = process.env.EVENTS_TABLE_NAME || 'events';
const USERS_TABLE_NAME = process.env.USERS_TABLE_NAME || 'users';
const GROUPS_TABLE_NAME = process.env.GROUPS_TABLE_NAME || 'groups';

const tableNames = { eventTable: EVENTS_TABLE_NAME, userTable: USERS_TABLE_NAME, groupTable: GROUPS_TABLE_NAME };

// MIDDLEWARE
setupCORS(app, FRONTEND_URL);
proxyAssets(app, NODE_ENV);
bodyParse(app);

// ROUTES
//? https://developer.mixpanel.com/reference/track-event
app.post('/track', async (req, res) => await handleMixpanelIncomingReq('track', req, res));
app.post('/engage', async (req, res) => await handleMixpanelIncomingReq('engage', req, res));
app.post('/groups', async (req, res) => await handleMixpanelIncomingReq('groups', req, res));
app.all('/', (req, res) => res.status(200).json({ status: "OK" }));
app.all('/ping', (req, res) => res.status(200).json({ status: "OK", message: "pong", version }));
app.all('/decide', (req, res) => res.status(299).send({ error: "the /decide endpoint is deprecated" }));
app.all('/drop', async (req, res) => await handleDrop(req, res));

// START by runtime
if (RUNTIME === 'LAMBDA') RUNTIME = 'AWS';
if (RUNTIME === 'FUNCTIONS') RUNTIME = 'AZURE';
if (RUNTIME === 'CLOUD_FUNCTIONS') RUNTIME = 'GCP';
if (RUNTIME === 'CLOUD_RUN') RUNTIME = 'LOCAL';
switch (RUNTIME) {
	case 'GCP':
		const { http } = require('@google-cloud/functions-framework');
		http('mixpanel_proxy', app);
		module.exports = app;
		break;
	case 'AWS':
		const serverless = require('serverless-http');
		module.exports.handler = serverless(app);
		break;
	case 'AZURE':
		const createHandler = require('azure-function-express').createHandler;
		module.exports = createHandler(app);
		break;
	default:
		app.listen(PORT, () => {
			log(`\n\nproxy alive on ${PORT}\n\n`);
		});
		break;
}

// in-use middleware + initialization
const activeMiddleware = DESTINATIONS
	.filter(wh => middleware[wh.toLowerCase()])
	.map(wh => ({ name: wh, api: middleware[wh.toLowerCase()] }));

for (const { name, api: middleware } of activeMiddleware) {
	if (middleware.init) {
		middleware.init(tableNames);
		log(`initializing ${name}`);
	}

}

/**
 * this function handles the incoming data from the Mixpanel JS lib
 * via .track() .people.set() and .group.set()
 * it also sends data to active warehouse middlewares
 * @param  {Endpoints} type
 * @param  {import('express').Request} req
 * @param  {import('express').Response} res
 */
async function handleMixpanelIncomingReq(type, req, res) {
	if (!type) return res.status(400).send('No type provided');
	if (!req.body) return res.status(400).send('No data provided');

	const data = parseSDKData(req.body?.data || req.body);
	const endUserIp = req.headers['x-forwarded-for'] || req?.socket?.remoteAddress || req?.connection?.remoteAddress;


	// mutations / transforms
	data.forEach(record => {
		// include the IP address for geo-location
		if (req.query.ip === '1') {
			if (type === 'track') record.properties.ip = endUserIp;
			if (type === 'engage') record.$ip = endUserIp;
			if (type === 'groups') record.$ip = endUserIp;
		}
	});

	const flatData = formatForWarehouse(data);

	const results = [];

	try {
		await Promise.all(activeMiddleware.map(async middleware => {
			const { name, api } = middleware;
			try {
				log(`sending ${type} data to ${name}`);
				const uploadData = middleware.name === 'mixpanel' ? data : clone(flatData);
				const status = await api(uploadData, type, tableNames);
				results.push({ name, status });
				return { name, status };
			}
			catch (e) {
				log(`error sending ${type} data to ${name}`, e);
				results.push({ name, status: e.message });
				return { name, status: `ERROR: ${e.message}` };
			}
		}));
		res.send(results);
	}

	catch (error) {
		if (RUNTIME === 'dev') console.error(error);
		res.status(500).send(`An error occurred calling /${type}`);
	}

	return results;
}


async function handleDrop(req, res) {
	const results = [];
	if (NODE_ENV === "prod") return res.status(400).send("Cannot drop tables in production");

	const drops = await Promise.all(activeMiddleware.map(async middleware => {
		const { name, api } = middleware;
		try {
			log(`DROPPING TABLES in ${name}`);
			const status = await api.drop(tableNames);
			results.push({ name, status });
			return { name, status };
		}
		catch (e) {
			log(`error dropping in ${name}`, e);
			results.push({ name, status: e.message });
			return { name, status: `ERROR: ${e.message}` };
		}
	}));

	res.send(results);
}


/**
 * prep data for each warehouse by basically flattening it + cleaning key names
 * also normalize certain values
 * todo: allow a user to define their own values to schematize
 * @param  {IncomingData} data
 * @returns {WarehouseData}
 */
function formatForWarehouse(data) {
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

					//convert time to ISO string
					else if (prop === 'time') {
						record.event_time = dayjs.unix(record.properties.time).toISOString();
						delete record.properties.time;
					}

					//todo: add more transformations here

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

module.exports.parseSDKData = parseSDKData;
module.exports.handleMixpanelData = handleMixpanelIncomingReq;
