// A row-level Mixpanel proxy server which receives data from Mixpanel's JS Lib and sends it to DWHs.
// by ak@mixpanel.com


// TYPES
/** @typedef {import('./types').Runtimes} Runtimes */
/** @typedef {import('./types').Destinations} Destinations */

// DEPENDENCIES
const express = require('express');
const app = express();

const { version } = require('./package.json');

// @ts-ignore
const fetch = require('fetch-retry')(global.fetch);

// CODE SPLITTING
const setupCORS = require('./components/corsConfig');
const proxyAssets = require('./components/proxyAssets');
const validateEnv = require('./components/validate');
const bodyParse = require('./components/bodyParse');
const { parseSDKData } = require('./components/parser');

// MIDDLEWARE
const bigquery = require('./middleware/bigquery');
const snowflake = require('./middleware/snowflake');
const redshift = require('./middleware/redshift');
const mixpanel = require('./middleware/mixpanel');
const middleware = { bigquery, snowflake, redshift, mixpanel };


// ENV
require('dotenv').config();
const NODE_ENV = process.env.NODE_ENV || 'prod';
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
validateEnv({ DESTINATIONS, RUNTIME, FRONTEND_URL, EVENTS_TABLE_NAME, USERS_TABLE_NAME, GROUPS_TABLE_NAME, ...process.env });

// MIDDLEWARE
setupCORS(app, FRONTEND_URL);
proxyAssets(app, RUNTIME);
bodyParse(app);

// ROUTES
app.post('/track', async (req, res) => await handleMixpanelData('track', req, res)); //? https://developer.mixpanel.com/reference/track-event
app.post('/engage', async (req, res) => await handleMixpanelData('engage', req, res)); //? https://developer.mixpanel.com/reference/engage
app.post('/groups', async (req, res) => await handleMixpanelData('groups', req, res)); //? https://developer.mixpanel.com/reference/groups
app.all('/', (req, res) => res.status(200).json({ status: "OK" }));
app.all('/ping', (req, res) => res.status(200).json({ status: "OK", message: "pong", version }));
app.all('/decide', (req, res) => res.status(299).send({ error: "the /decide endpoint is deprecated" }));

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
			if (RUNTIME === 'dev') console.log(`proxy alive on ${PORT}`);
		});
		break;
}

// in-use middleware
const activeMiddleware = DESTINATIONS
	.filter(wh => middleware[wh.toLowerCase()])
	.map(wh => ({ name: wh, api: middleware[wh.toLowerCase()] }));


/**
 * this function handles the incoming data from the Mixpanel JS lib
 * via .track() .people.set() and .group.set()
 * it also sends data to active warehouse middlewares
 * @param  {'track' | 'engage' | 'groups'} type
 * @param  {import('express').Request} req
 * @param  {import('express').Response} res
 */
async function handleMixpanelData(type, req, res) {
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

	const results = [];

	try {
		await Promise.all(activeMiddleware.map(async middleware => {
			const { name, api } = middleware;
			try {
				const status = await api(data, type);
				results.push({ name, status });
				return { name, status };
			}
			catch (e) {
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


module.exports.parseSDKData = parseSDKData;
module.exports.handleMixpanelData = handleMixpanelData;
