// @ts-nocheck
/**
 * unit tests for the pubsub destination's topic-ensure caching and publish
 * results, with the GCP client mocked (no cloud creds needed).
 *
 * the topic cache matters because TWO callers share this module's state:
 * the destination fan-out (real table names) and the dead-letter component
 * (empty table names + pubsub_bad_topic). neither caller's init may mask
 * the other's topics.
 */

let mockExistingTopics = [];
const mockClient = {
	getTopics: jest.fn(async () => [mockExistingTopics.map((t) => ({ name: `projects/test/topics/${t}` }))]),
	createTopic: jest.fn(async (name) => {
		mockExistingTopics.push(name);
		return [{}];
	}),
	topic: jest.fn(() => ({
		publishMessage: jest.fn(async () => 'mock-message-id'),
		delete: jest.fn(async () => {}),
	})),
};

jest.mock('@google-cloud/pubsub', () => ({
	PubSub: jest.fn(() => mockClient),
}));

const pubsub = require('../middleware/pubsub');

beforeAll(() => {
	process.env.pubsub_project = 'test-project';
	process.env.pubsub_bad_topic = 'dlq';
});

describe('PUBSUB TOPIC CACHE', () => {
	test('dead-letter init (no table names) ensures only the bad topic', async () => {
		await pubsub.init({ eventTable: '', userTable: '', groupTable: '' });
		expect(mockExistingTopics).toContain('dlq');
		expect(mockExistingTopics).not.toContain('events');
	});

	test('destination init STILL creates per-type topics after dead-letter init', async () => {
		await pubsub.init({ eventTable: 'events', userTable: 'users', groupTable: 'groups' });
		expect(mockExistingTopics).toEqual(expect.arrayContaining(['dlq', 'events', 'users', 'groups']));
	});

	test('already-ensured topics are not re-created', async () => {
		mockClient.createTopic.mockClear();
		await pubsub.init({ eventTable: 'events', userTable: 'users', groupTable: 'groups' });
		expect(mockClient.createTopic).not.toHaveBeenCalled();
	});
});

describe('PUBSUB PUBLISH RESULTS', () => {
	test('publishing a batch array reports insertedRows = batch length', async () => {
		const result = await pubsub.publish([{ a: 1 }, { b: 2 }], 'events');
		expect(result.status).toBe('success');
		expect(result.insertedRows).toBe(2);
	});

	test('publishing a single object reports insertedRows = 1', async () => {
		const result = await pubsub.publish({ table: 'events', error: 'boom' }, 'dlq');
		expect(result.status).toBe('success');
		expect(result.insertedRows).toBe(1);
	});
});
