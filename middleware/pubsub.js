/*
----
PUBSUB MIDDLEWARE
----
*/

//todo: testing + types

const { PubSub } = require("@google-cloud/pubsub");
/** @typedef {import('@google-cloud/pubsub').PubSub} PubSubClient */

const NODE_ENV = process.env.NODE_ENV || "prod";
const u = require("ak-tools");
// const { schematizeForWarehouse } = require('../components/transforms.js');
const { insertWithRetry } = require("../components/retries.js");
const log = require("../components/logger.js");

if (NODE_ENV === 'test') {
	log.verbose(true);
	log.cli(true);
}

// CORE MIDDLEWARE CONTRACT
/** @typedef {import('../types').Entities} Entities */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TopicNames */
/** @typedef {import('../types').PubSubMessage} PubSubMessage */
/** @typedef {import('../types').PublishResult} PublishResult */

// these vars should be cached and only run once when the server starts
/** @type {PubSubClient} */
let client;

let pubsub_project;
let pubsub_keyfile;
let pubsub_service_account_email;
let pubsub_service_account_private_key;
let isClientReady;
let areTopicsReady;

/**
 * Main function to handle Pub/Sub message publishing
 * this function is called in the main server.js file
 * and will be called repeatedly as clients stream data in (from client-side SDKs)
 * @param  {PubSubMessage} data
 * @param  {Endpoints} type
 * @param  {TopicNames} topicNames
 * @return {Promise<PublishResult>}
 */
async function main(data, type, topicNames) {
	const startTime = Date.now();
	const init = await initializePubSub(topicNames);

	if (!init.every(i => i)) throw new Error("Failed to initialize Pub/Sub middleware.");

	const { eventTable: eventTopic, userTable: userTopic, groupTable: groupTopic } = topicNames;
	let targetTopic;

	switch (type) {
		case "track":
			targetTopic = eventTopic;
			break;
		case "engage":
			targetTopic = userTopic;
			break;
		case "groups":
			targetTopic = groupTopic;
			break;
		default:
			throw new Error("Invalid Record Type");
	}

	// @ts-ignore
	const result = await insertWithRetry(publishMessage, data, targetTopic);
	const duration = Date.now() - startTime;
	result.duration = duration;

	return result;
}

async function initializePubSub(topicNames) {
	// ENV STUFF
	({ pubsub_project, pubsub_keyfile, pubsub_service_account_email, pubsub_service_account_private_key } =
		process.env);
	const { eventTable: eventTopic, userTable: userTopic, groupTable: groupTopic } = topicNames;

	if (!isClientReady) {
		isClientReady = await verifyPubSubCredentials();
		if (!isClientReady) throw new Error("Pub/Sub credentials verification failed.");
	}

	if (!areTopicsReady) {
		const topicCheckResults = await verifyOrCreateTopics([eventTopic, userTopic, groupTopic]);
		areTopicsReady = topicCheckResults.every(result => result);
		if (!areTopicsReady) throw new Error("Topic verification or creation failed.");
	}

	return [isClientReady, areTopicsReady];
}

async function verifyPubSubCredentials() {
	// credentials or keyfile or application default credentials
	/** @type {import('@google-cloud/pubsub').ClientConfig} */
	const auth = {};

	if (pubsub_keyfile) {
		auth.keyFilename = pubsub_keyfile;
	}
	if (pubsub_project) auth.projectId = pubsub_project;

	if (pubsub_service_account_email && pubsub_service_account_private_key) {
		auth.credentials = {
			client_email: pubsub_service_account_email,
			private_key: pubsub_service_account_private_key,
		};
	}

	client = new PubSub(auth);

	try {
		await client.getTopics(); // Simple request to verify client setup
		log("[PUBSUB] credentials are valid");
		return true;
	} catch (error) {
		log("[PUBSUB] Error verifying Pub/Sub credentials:", error);
		return false;
	}
}

/**
 * @param {string[]} topicNames
 */
async function verifyOrCreateTopics(topicNames) {
	const results = [];

	const [topics] = await client.getTopics();
	const topicIds = topics.map(topic => topic.name.split('/').pop());

	for (const topic of topicNames) {
		if (!topicIds.includes(topic)) {
			log(`[PUBSUB] Topic ${topic} does not exist. Creating...`);
			await client.createTopic(topic);

			const [updatedTopics] = await client.getTopics();
			const updatedTopicIds = updatedTopics.map(t => t.name.split('/').pop());

			if (updatedTopicIds.includes(topic)) {
				log(`[PUBSUB] Topic ${topic} created.`);
				results.push(true);
			} else {
				log(`[PUBSUB] Failed to create topic ${topic}`);
				results.push(false);
			}
		} else {
			log(`[PUBSUB] Topic ${topic} already exists.`);
			results.push(true);
		}
	}

	return results;
}

/**
 * Publishes a message to a Pub/Sub topic
 * @param  {PubSubMessage} message
 * @param  {string} topicName
 * @return {Promise<PublishResult>}
 */
async function publishMessage(message, topicName) {
	log("[PUBSUB] Starting message publishing...");

	const topic = client.topic(topicName);
	let result = { status: "born" };

	try {
		const dataBuffer = Buffer.from(JSON.stringify(message));
		const messageId = await topic.publishMessage({ data: dataBuffer });
		result = { status: "success", messageId };
	} catch (error) {
		if (NODE_ENV === 'test') debugger;

		log(`[PUBSUB] Error publishing message to topic ${topicName}:`, error);
		result = {
			status: "error",
			error: error.message
		};
	}

	log("[PUBSUB] Message publishing complete.");
	return { ...result };
}

/**
 * Drops all specified Pub/Sub topics... this is a destructive operation
 * @param  {TopicNames} topicNames
 */
async function dropTopics(topicNames) {
	log("[PUBSUB] Dropping all topics...");

	const { eventTable: eventTopic, userTable: userTopic, groupTable: groupTopic } = topicNames;
	const topicsToDrop = [eventTopic, userTopic, groupTopic];
	const dropPromises = topicsToDrop.map(async (topicName) => {
		const topic = client.topic(topicName);
		await topic.delete();
		log(`[PUBSUB] Dropped topic: ${topicName}`);
	});

	await Promise.all(dropPromises);
	log(`[PUBSUB] Dropped ${topicsToDrop.length} topics: ${topicsToDrop.join(", ")}`);
	return { numTopicsDropped: topicsToDrop.length, topicsDropped: topicsToDrop };
}

main.drop = dropTopics;
main.init = initializePubSub;

module.exports = main;
