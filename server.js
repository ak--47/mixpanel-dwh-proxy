// runtime shell: picks the right export/listen strategy per platform.
// all express wiring + request handling lives in app.js
// by ak@mixpanel.com

const { app } = require('./app');
const log = require('./components/logger');

/** @typedef {import('./types').Runtimes} Runtimes */
/** @type {Runtimes} */
let RUNTIME = process.env.RUNTIME?.toUpperCase() || 'LOCAL';

// START by runtime
const PORT = process.env.PORT || 8080;
if (RUNTIME === 'LAMBDA') RUNTIME = 'AWS';
if (RUNTIME === 'FUNCTIONS') RUNTIME = 'AZURE';
if (RUNTIME === 'CLOUD_FUNCTIONS') RUNTIME = 'GCP';
if (RUNTIME === 'CLOUD_RUN') RUNTIME = 'LOCAL';
switch (RUNTIME) {
	case 'GCP':
		const { http } = require('@google-cloud/functions-framework');
		http('start', app);
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
			log(`---- proxy alive on ${PORT} ---- `);
		});
		break;
}
