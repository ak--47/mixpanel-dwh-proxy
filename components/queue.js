require('dotenv').config({ override: false });
const QUEUE_MAX = parseInt(process.env.QUEUE_MAX || "0", 10) || 0; // 0 = no queue
const QUEUE_INTERVAL = parseInt(process.env.QUEUE_INTERVAL || `${60 * 15}`, 10) || 60 * 15; // ensure queue is flushed every 15 minutes
const QUEUE_EVENTS = [];
const QUEUE_USERS = [];
const QUEUE_GROUPS = [];
let lastFlushTime = Date.now();

async function flushQueue(queue, type, handleMixpanelRequest) {
	if (queue.length > 0) {
		const itemsToFlush = queue.splice(0, queue.length);
		const dataToFlush = itemsToFlush.map(item => item.data);
		const headers = itemsToFlush.length > 0 ? itemsToFlush[0].headers : {};
		const result = await handleMixpanelRequest(type, { body: dataToFlush, headers }, { send: () => { } });
		return result;
	}
}

async function checkAndFlushQueues(handleMixpanelRequest, force = false) {
	const currentTime = Date.now();
	if (currentTime - lastFlushTime > QUEUE_INTERVAL * 1000 || force) {
		const result = await Promise.all([
			flushQueue(QUEUE_EVENTS, 'track', handleMixpanelRequest),
			flushQueue(QUEUE_USERS, 'engage', handleMixpanelRequest),
			flushQueue(QUEUE_GROUPS, 'groups', handleMixpanelRequest),
		]);
		lastFlushTime = currentTime;
		return result;
	} else {
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

			if (queue.length >= QUEUE_MAX) {
				const flushResults = await flushQueue(queue, type, handleMixpanelRequest);
				if (flushResults) return res.status(200).header('Content-Type', 'application/json').send(flushResults);
				//END HERE?!?!?
			}

			const results = await checkAndFlushQueues(handleMixpanelRequest);
			if (!results) return res.status(200).header('Content-Type', 'application/json').send({ type: type.slice(0), status: 'queued' });
			return res.status(200).header('Content-Type', 'application/json').send(results);
		}
		else {
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

module.exports = queue;
