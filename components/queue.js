require('dotenv').config({ override: false });
const NODE_ENV = process.env.NODE_ENV || "prod";
const log = require("../components/logger.js");
if (NODE_ENV === 'test') {
	log.verbose(true);
	log.cli(true);
}

const QUEUE_MAX = parseInt(process.env.QUEUE_MAX || "0", 10) || 0; // 0 = no queue
const QUEUE_INTERVAL = parseInt(process.env.QUEUE_INTERVAL || `${60 * 10}`, 10) || 60 * 10; // ensure queue is flushed every 10 minutes
const QUEUE_EVENTS = [];
const QUEUE_USERS = [];
const QUEUE_GROUPS = [];
let lastFlushTime = Date.now();

async function flushQueue(queue, type, handleMixpanelRequest) {
	if (queue.length > 0) {
		const itemsToFlush = queue.splice(0, queue.length);
		const dataToFlush = itemsToFlush.map(item => item.data);
		const headers = itemsToFlush.length > 0 ? itemsToFlush[0].headers : {};
		log(`[QUEUE] flushing ${dataToFlush.length} ${type} records`);
		const result = await handleMixpanelRequest(type, { body: dataToFlush, headers }, { send: () => { } });
		log(`[QUEUE] flushed ${dataToFlush.length} ${type} records`);
		return result;
	}
}

async function checkQueue(handleMixpanelRequest, force = false) {
	const currentTime = Date.now();
	if ((currentTime - lastFlushTime > (QUEUE_INTERVAL * 1000)) || force) {
		log(`[QUEUE] cache expired flushing all queues`);
		const result = await Promise.all([
			flushQueue(QUEUE_EVENTS, 'track', handleMixpanelRequest),
			flushQueue(QUEUE_USERS, 'engage', handleMixpanelRequest),
			flushQueue(QUEUE_GROUPS, 'groups', handleMixpanelRequest),
		]);
		log(`[QUEUE] all queues flushed`);
		lastFlushTime = currentTime;
		return result;
	} else {
		log(`[QUEUE] cache not expired, not flushing queues`);
		return null;
	}
}

function queue(type, handleMixpanelRequest) {
	return async (req, res, next) => {
		if (QUEUE_MAX > 0) {
			const queue = getQueueByType(type);
			const headers = req.headers;

			if (Array.isArray(req.body)) {
				req.body.forEach((item) => queue.push({ data: item, headers }));
			} else {
				queue.push({ data: req.body, headers });
			}

			// If the queue is full, flush it immediately
			if (queue.length >= QUEUE_MAX) {
				log(`[QUEUE] ${type} queue is full`);
				const flushResults = await flushQueue(queue, type, handleMixpanelRequest);
				log(`[QUEUE] ${type} queue is empty (capacity-based flush)`);
				if (flushResults) return res.status(200).header('Content-Type', 'application/json').send(flushResults);
			}

			// Check if it's time to flush the queue
			await checkQueue(handleMixpanelRequest);
			return res.status(200).header('Content-Type', 'application/json').send({ type: type, status: 'queued' });
		} else {
			next();
		}
	};
}

function getQueueByType(type) {
	switch (type) {
		case 'track':
			return QUEUE_EVENTS;
		case 'engage':
			return QUEUE_USERS;
		case 'groups':
			return QUEUE_GROUPS;
		default:
			return [];
	}
}

function queueMiddleware(handleMixpanelRequest) {
	return async (req, res, next) => {
		if (QUEUE_MAX > 0) {
			await checkQueue(handleMixpanelRequest);
		}
		next();
	};
}

module.exports = {
	queue,
	checkQueue,
	queueMiddleware
};
