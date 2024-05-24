/**
 * parses the incoming data from the SDK
 * @param  {string | Object | Object[]} reqBody
 * @returns {Object[]}
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

module.exports = { parseSDKData };