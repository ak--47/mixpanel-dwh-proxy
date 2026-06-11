/**
 * @fileoverview dead-letter component: publishes failed inserts to the
 * pubsub_bad_topic so bad rows are captured instead of silently dropped.
 * any destination can use it. configured by pubsub_bad_topic alone, and
 * send() NEVER throws — dead-lettering is best-effort and must not mask
 * the original insert error.
 */

const pubsub = require('../middleware/pubsub');
const log = require('./logger');

let isTopicReady = false;

function isConfigured() {
	return !!process.env.pubsub_bad_topic;
}

/**
 * publish a failure report to the dead-letter topic;
 * no-op when pubsub_bad_topic is unset
 * @param {Object} message anything JSON-serializable (failed rows + error context)
 * @returns {Promise<Object|null>} the publish result, or null when unconfigured/failed
 */
async function send(message) {
	if (!isConfigured()) return null;
	const topic = process.env.pubsub_bad_topic;
	try {
		if (!isTopicReady) {
			// empty table names: init then only ensures the good/bad topics from env
			await pubsub.init({ eventTable: '', userTable: '', groupTable: '' });
			isTopicReady = true;
		}
		const result = await pubsub.publish(message, topic);
		log(`[DEAD LETTER] published to topic ${topic}; message id: ${result?.messageId}`);
		return result;
	}
	catch (error) {
		log(`[DEAD LETTER] failed to publish to topic ${topic}`, error);
		return null;
	}
}

module.exports = { isConfigured, send };
