// @ts-nocheck
// unit tests for components/deadLetter.js (mocked pubsub, no cloud creds)

jest.mock('../middleware/pubsub', () => {
	const main = jest.fn(async () => ({ status: 'success' }));
	return Object.assign(main, {
		init: jest.fn(async () => [true, true]),
		drop: jest.fn(),
		publish: jest.fn(async (message, topic) => ({ status: 'success', messageId: 'msg-123' })),
		requiredEnvVars: ['pubsub_project'],
	});
});

const pubsub = require('../middleware/pubsub');
const deadLetter = require('../components/deadLetter');

const OLD_ENV = process.env;

beforeEach(() => {
	jest.clearAllMocks();
	process.env = { ...OLD_ENV };
	delete process.env.pubsub_bad_topic;
});

afterAll(() => {
	process.env = OLD_ENV;
});

describe('DEAD LETTER', () => {

	test('isConfigured false without pubsub_bad_topic', () => {
		expect(deadLetter.isConfigured()).toBe(false);
	});

	test('isConfigured true with pubsub_bad_topic', () => {
		process.env.pubsub_bad_topic = 'my-bad-topic';
		expect(deadLetter.isConfigured()).toBe(true);
	});

	test('send is a no-op when unconfigured', async () => {
		const result = await deadLetter.send({ error: 'whoops' });
		expect(result).toBeNull();
		expect(pubsub.publish).not.toHaveBeenCalled();
	});

	test('send publishes the message to the bad topic', async () => {
		process.env.pubsub_bad_topic = 'my-bad-topic';
		const message = { table: 'events', error: 'whoops', data: [{ foo: 'bar' }] };
		const result = await deadLetter.send(message);
		expect(pubsub.init).toHaveBeenCalled();
		expect(pubsub.publish).toHaveBeenCalledWith(message, 'my-bad-topic');
		expect(result.messageId).toBe('msg-123');
	});

	test('send never throws, even when publish fails', async () => {
		process.env.pubsub_bad_topic = 'my-bad-topic';
		pubsub.publish.mockRejectedValueOnce(new Error('pubsub is down'));
		await expect(deadLetter.send({ error: 'whoops' })).resolves.toBeNull();
	});

});
