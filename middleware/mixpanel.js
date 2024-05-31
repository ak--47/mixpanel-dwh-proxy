// @ts-ignore
const fetch = require('fetch-retry')(global.fetch);
const log = require('../components/logger');

const NODE_ENV = process.env.NODE_ENV || 'prod';
const REGION = process.env.MIXPANEL_REGION || 'US';
const BASE_URL = `https://api${REGION?.toUpperCase() === "EU" ? '-eu' : ''}.mixpanel.com`;
if (!BASE_URL) throw new Error('BASE_URL is required; mixpanel middleware is not ready');
if (NODE_ENV === 'test') {
	log.verbose(true);
	log.cli(true);
}

/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').InsertResult} InsertResult */
/** @typedef {import('../types').MixpanelEvent} Event */
/** @typedef {import('../types').UserUpdate} UserUpdate */
/** @typedef {import('../types').GroupUpdate} GroupUpdate */
/** @typedef {Event[] | UserUpdate[] | GroupUpdate[]} DATA */


/**
 * sends a POST request to the given URL with the given data
 * @param  {DATA} data
 * @param  {Endpoints} type
 * @returns {Promise<InsertResult>}
 */
async function main(data, type,) {
	const start = Date.now();
	const url = `${BASE_URL}/${type}?verbose=1`;
	log(`[MIXPANEL] request to ${shortUrl(url)}`);
	try {
		const request = await fetch(url, {
			method: 'POST',
			body: JSON.stringify(data),
			headers: {
				'Content-Type': 'application/json'
			},
		});

		const { status = 0, statusText = "" } = request;
		const response = await request.json();
		const duration = Date.now() - start;
		response.duration = duration;
		log(`[MIXPANEL] got ${status} ${statusText} from ${shortUrl(url)}`);
		const result = { status: "success", duration, failedRows: 0, insertedRows: data.length };
		if (response.error) {
			result.status = "error";
			result.errorMessage = response.error;
		}
		return result;
	}
	catch (error) {
		const duration = Date.now() - start;
		log(`[MIXPANEL] error in makeRequest: ${error}`, error);
		return { status: "error", duration, errorMessage: error.message };
	}
}




// helpers
function shortUrl(url) {
	return new URL(url).pathname;
}

main.init = () => {
	log(`[MIXPANEL] middleware initialized in ${REGION} region with base URL ${BASE_URL}`);
};
main.drop = () => {
	log(`[MIXPANEL] tables cannot be dropped...0_o`);
	return "nothing to drop";
};
module.exports = main;
