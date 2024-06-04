const { sleep } = require("ak-tools");
const log = require("../components/logger.js");

const NODE_ENV = process.env.NODE_ENV || "prod";
let MAX_RETRIES = process.env.MAX_RETRIES || 5;
if (typeof MAX_RETRIES === "string") MAX_RETRIES = parseInt(MAX_RETRIES);
/** @type {number} */
let maxRetries = MAX_RETRIES;

if (NODE_ENV === "test") {
	log.verbose(true);
	log.cli(true);
}

/** @typedef {import('../types').Schema} Schema */
/** @typedef {import('../types').InsertResult} InsertResult */
/** @typedef {import('../types').SchematizedData} WarehouseData */
/** @typedef {import('../types').FlatData} FlatData */

/**
 * a function that retries an insert operation
 * @param {function} fn an insert function
 * @param  {WarehouseData | FlatData} batch a batch of data, either flat or schematized
 * @param  {string} table the table to insert to
 * @param  {Schema} schema the schema
 * @param  {string[]} retryableErrors an array of error messages to trigger a retry
 * @param  {number[]} retryableStatusCodes an array of HTTP status codes to trigger a retry
 * @param  {function} backoffStrategy a function to calculate backoff time
 * @return {Promise<InsertResult>} the result of the insert operation
 */
async function insertWithRetry(
	fn, batch, table, schema = [],
	retryableErrors = ["TableLockedError", "LockNotAvailableError", "NetworkError"],
	retryableStatusCodes = [429, 500, 503],
	backoffStrategy = (attempt) => Math.min(1000 * 2 ** attempt, 30000)
) {
	let attempt = 0;

	while (attempt < maxRetries) {
		try {
			const result = await fn(batch, table, schema);
			return result;
		} catch (error) {
			const isRetryableError = retryableErrors.includes(error.message);
			const isRetryableStatusCode = (error.statusCode || error.code) && retryableStatusCodes.includes(error.statusCode);

			if (isRetryableError || isRetryableStatusCode) {
				const waitTime = backoffStrategy(attempt);
				log(`[${error.message.toUpperCase()}] retry attempt #${attempt + 1} (waiting ${waitTime} ms) `);
				await sleep(waitTime);
				attempt++;
			} else {
				throw error;
			}
		}
	}

	throw new Error(`Failed to insert data after ${MAX_RETRIES} attempts`);
}

module.exports = {
	insertWithRetry,
};
