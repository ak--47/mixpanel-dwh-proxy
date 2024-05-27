// @ts-ignore
const fetch = require('fetch-retry')(global.fetch);
const log = require('../components/logger');

const NODE_ENV = process.env.NODE_ENV || 'prod';
const REGION = process.env.REGION || 'US';
const BASE_URL = `https://api${REGION?.toUpperCase() === "EU" ? '-eu' : ''}.mixpanel.com`;
if (!BASE_URL) throw new Error('BASE_URL is required; mixpanel middleware is not ready');

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
 */
async function main(data, type,) {
	const url = `${BASE_URL}/${type}?verbose=1`;
	log(`\nrequest to ${shortUrl(url)} with data:\n${pp(data)} ${sep()}`);
	let result = { status: "born", destination: "mixpanel" };
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

		log(`got ${status} ${statusText} from ${shortUrl(url)}:\n${pp(response)} ${sep()}`);

		return response;
	}
	catch (error) {
		console.error(`error in makeRequest: ${error}`);
		return { error };
	}
}




// helpers
function pp(obj) {
	return JSON.stringify(obj, null, 2);
}

function sep() {
	return `\n--------\n`;
}

function shortUrl(url) {
	return new URL(url).pathname;
}

main.init = () => {
	log(`mixpanel middleware initialized`);
};
main.drop = () => {
	log(`mixpanel tables cannot be dropped...yet`);
	return "nothing to drop";
}
module.exports = main;
