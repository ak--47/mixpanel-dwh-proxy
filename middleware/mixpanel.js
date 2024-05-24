const NODE_ENV = process.env.NODE_ENV || 'prod';
const REGION = process.env.REGION || 'US';
const BASE_URL = `https://api${REGION?.toUpperCase() === "EU" ? '-eu' : ''}.mixpanel.com`;
if (!BASE_URL) throw new Error('BASE_URL is required; mixpanel middleware is not ready');

/**
 * sends a POST request to the given URL with the given data
 * @param  {Object[]} data
 * @param  {string} type
 * @param  {string} base
 */
async function makeRequest(data, type, base = BASE_URL) {
	const url = `${base}/${type}?verbose=1`;
	if (NODE_ENV === 'dev') console.log(`\nrequest to ${shortUrl(url)} with data:\n${pp(data)} ${sep()}`);
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

		if (NODE_ENV === 'dev') console.log(`got ${status} ${statusText} from ${shortUrl(url)}:\n${pp(response)} ${sep()}`);

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


module.exports = makeRequest;
