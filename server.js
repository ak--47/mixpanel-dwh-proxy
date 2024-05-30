// A row-level Mixpanel proxy server which receives data from Mixpanel's JS Lib and sends it to DWHs.
// by ak@mixpanel.com

//todos: azure blob + azure functions
// better logging labels depending on which middleware is running

// TYPES
/** @typedef {import('./types').Runtimes} Runtimes */
/** @typedef {import('./types').Destinations} Destinations */
/** @typedef {import('./types').Warehouse} Warehouse */
/** @typedef {import('./types').Lake} Lake */
/** @typedef {import('./types').Endpoints} Endpoints */
/** @typedef {import('./types').IncomingData} IncomingData */
/** @typedef {import('./types').FlatData} WarehouseData */
/** @typedef {import('./types').TableNames} TableNames */

// DEPENDENCIES
const express = require('express');
const app = express();
const { version } = require('./package.json');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);
const { clone } = require('ak-tools');


// CODE SPLITTING
const setupCORS = require('./components/corsConfig');
const proxyAssets = require('./components/proxyAssets');
const validateEnv = require('./components/validate');
const bodyParse = require('./components/bodyParse');
const { parseSDKData, flattenAndRenameForWarehouse, schematizeForWarehouse } = require('./components/transforms');

const log = require('./components/logger');

// MIDDLEWARE
const bigquery = require('./middleware/bigquery');
const snowflake = require('./middleware/snowflake');
const redshift = require('./middleware/redshift');
const mixpanel = require('./middleware/mixpanel');
const gcs = require('./middleware/gcs');
const s3 = require('./middleware/s3');
const azure = require('./middleware/azure');
const middleware = { bigquery, snowflake, redshift, mixpanel, gcs, s3, azure };



// ENV VARS + CONFIG
require('dotenv').config({ override: false });
const PARAMS = validateEnv();
const NODE_ENV = process.env.NODE_ENV || 'prod';
if (NODE_ENV === 'dev') { log.verbose(true); log.cli(true); } // log everything
if (NODE_ENV === 'prod') { log.verbose(false); log.cli(false); } //only logs structured logs + error
log(`running in ${NODE_ENV} mode; version: ${version}; verbose: ${log.isVerbose()} cli: ${log.isCli()}`);

const WAREHOUSES = process.env.WAREHOUSES || "MIXPANEL";
const warehouseList = WAREHOUSES.split(',').map(wh => wh.trim()).filter(a => a);
const LAKE = process.env.LAKE || "";
const lakeList = LAKE.split(',').map(wh => wh.trim()).filter(a => a);
const EVENTS_TABLE_NAME = process.env.EVENTS_TABLE_NAME || 'events';
const USERS_TABLE_NAME = process.env.USERS_TABLE_NAME || 'users';
const GROUPS_TABLE_NAME = process.env.GROUPS_TABLE_NAME || 'groups';
const MIXPANEL_TOKEN = process.env.MIXPANEL_TOKEN || "";

/** @type {Destinations[]} */
const DESTINATIONS = [...warehouseList, ...lakeList].flat().filter(a => a).map(t => t.toLowerCase());
/** @type {Runtimes} */
let RUNTIME = process.env.RUNTIME?.toUpperCase() || 'LOCAL';
/** @type {TableNames} */
const TABLE_NAMES = { eventTable: EVENTS_TABLE_NAME, userTable: USERS_TABLE_NAME, groupTable: GROUPS_TABLE_NAME };

// MIDDLEWARE
let FRONTEND_URL = process.env.FRONTEND_URL || "";
if (FRONTEND_URL === "none") FRONTEND_URL = "";
setupCORS(app, FRONTEND_URL);
proxyAssets(app, NODE_ENV);
bodyParse(app);

// ROUTES
//? https://developer.mixpanel.com/reference/track-event
app.post('/track', async (req, res) => await handleMixpanelRequest('track', req, res));
app.post('/engage', async (req, res) => await handleMixpanelRequest('engage', req, res));
app.post('/groups', async (req, res) => await handleMixpanelRequest('groups', req, res));
app.all('/', (req, res) => res.status(200).json({ status: "OK" }));
app.all('/ping', (req, res) => res.status(200).json({ status: "OK", message: "pong", version }));
app.all('/decide', (req, res) => res.status(299).send({ error: "the /decide endpoint is deprecated" }));
app.all('/drop', async (req, res) => await handleDrop(req, res));

// START by runtime
const PORT = process.env.PORT || 8080;
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
		middleware.init(TABLE_NAMES); //these methods are async, but we don't want to wait for them.
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
async function handleMixpanelRequest(type, req, res) {
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

		// include token
		if (MIXPANEL_TOKEN) {
			if (type === 'track') record.properties.token = MIXPANEL_TOKEN;
			if (type === 'engage') record.$token = MIXPANEL_TOKEN;
			if (type === 'groups') record.$token = MIXPANEL_TOKEN;
		}
	});

	const flatData = flattenAndRenameForWarehouse(data);

	const results = [];

	try {
		const flush = await Promise.all(activeMiddleware.map(async middleware => {
			const { name, api } = middleware;
			try {
				log(`sending ${type} data to ${name}`);
				const uploadData = middleware.name === 'mixpanel' ? data : clone(flatData);
				const result = await api(uploadData, type, TABLE_NAMES);
				results.push({ name, result });
				return { name, result };
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
	if (NODE_ENV === "prod") return res.status(403).send("Cannot drop tables in production");

	const drops = await Promise.all(activeMiddleware.map(async middleware => {
		const { name, api } = middleware;
		try {
			log(`DROPPING TABLES in ${name}`);
			const status = await api.drop(TABLE_NAMES);
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



// module.exports.parseSDKData = parseSDKData;
// module.exports.handleMixpanelData = handleMixpanelRequest;
