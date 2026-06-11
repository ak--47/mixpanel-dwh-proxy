require('dotenv').config({ override: false });
const validateEnv = require('../components/validate');
const { flattenAndRenameForWarehouse } = require('../components/transforms');
const isDebugMode = process.env.NODE_OPTIONS?.includes('--inspect') || process.env.NODE_OPTIONS?.includes('--inspect-brk');
jest.setTimeout(isDebugMode ? 3600000 : 60000);

/** @typedef {import('../types').EnvVars} Params */

/** @type {Params} */
// @ts-ignore
const PARAMS = validateEnv();

// MIDDLEWARE (see middleware/index.js registry)
const registry = require('../middleware');
const { bigquery, snowflake, redshift, mixpanel, gcs, s3, azure, pubsub } = registry;

// a destination's tests only run when its required env vars are present;
// otherwise the whole describe block is skipped (creds for S3/Redshift may not exist)
// NOTE: validateEnv() above mirrors case-insensitive vars into process.env first
const hasCreds = (name) => registry[name].requiredEnvVars.every((v) => !!process.env[v]);
const describeIf = (condition) => (condition ? describe : describe.skip);

const timeout = 60000;
const { EVENTS_TABLE_NAME = "", USERS_TABLE_NAME = "", GROUPS_TABLE_NAME = "" } = PARAMS;
const tableNames = { eventTable: EVENTS_TABLE_NAME, userTable: USERS_TABLE_NAME, groupTable: GROUPS_TABLE_NAME };


let originalEnv;
beforeEach(() => {
	// Save the original process.env
	originalEnv = { ...process.env };
});

afterEach(() => {
	// Restore the original process.env
	process.env = originalEnv;
});

/** @type {import('../types').MixpanelEvent[]} */
const eventData = [{ "event": "foo", properties: { time: Date.now(), "token": "bar", "$insert_id": "baz", "$device_id": "qux", "hello": "world" } }];

/** @type {import('../types').UserUpdate[]} */
// @ts-ignore
const userData = [{ $distinct_id: "foo", $token: "hello", $set: { "hello": "world" } }];

/** @type {import('../types').GroupUpdate[]} */
// @ts-ignore
const groupData = [{ $token: "hello", $group_key: "world", $group_id: "foo", "$set": { "hello": "world" } }];

const e = flattenAndRenameForWarehouse(eventData);
const u = flattenAndRenameForWarehouse(userData);
const g = flattenAndRenameForWarehouse(groupData);


describeIf(hasCreds('bigquery'))('BigQuery', () => {


	test('bq: events', async () => {
		const result = await bigquery(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('bq: users', async () => {
		const result = await bigquery(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('bq: groups', async () => {
		const result = await bigquery(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('bq: pub sub fallback', async () => {
		const unnamed = { time: new Date().toISOString(), "token": "bar", "$insert_id": "baz", "$device_id": "qux", "hello": "world" };
		process.env.pubsub_bad_topic = 'test-bad-topic';
		// @ts-ignore
		const result = await bigquery([...e, unnamed], 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(2);
		expect(insertedRows).toBe(0);
		expect(status).toBe('error');

		// @ts-ignore
		expect(result?.type).toBe('partial failure');
	});

});

describeIf(hasCreds('snowflake'))('Snowflake', () => {

	test('snowflake: events', async () => {
		const result = await snowflake(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('snowflake: users', async () => {
		const result = await snowflake(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('snowflake: groups', async () => {
		const result = await snowflake(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

});

describeIf(hasCreds('redshift'))('Redshift', () => {

	test('redshift: events', async () => {
		const result = await redshift(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('redshift: users', async () => {
		const result = await redshift(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('redshift: groups', async () => {
		const result = await redshift(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

});

describeIf(hasCreds('mixpanel'))('Mixpanel', () => {

	test('mixpanel: events', async () => {
		const result = await mixpanel(eventData, 'track');
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('mixpanel: users', async () => {
		const result = await mixpanel(userData, 'engage');
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('mixpanel: groups', async () => {
		const result = await mixpanel(groupData, 'groups');
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);
});


describeIf(hasCreds('gcs'))('GCS', () => {

	test('gcs: events', async () => {
		const result = await gcs(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('gcs: users', async () => {
		const result = await gcs(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('gcs: groups', async () => {
		const result = await gcs(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);
});

describeIf(hasCreds('s3'))('S3', () => {

	test('s3: events', async () => {
		const result = await s3(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('s3: users', async () => {
		const result = await s3(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('s3: groups', async () => {
		const result = await s3(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);
});

describeIf(hasCreds('azure'))('Azure', () => {

	test('azure: events', async () => {
		const result = await azure(e, 'track', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('azure: users', async () => {
		const result = await azure(u, 'engage', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);

	test('azure: groups', async () => {
		const result = await azure(g, 'groups', tableNames);
		const { failedRows, insertedRows, status } = result;
		expect(failedRows).toBe(0);
		expect(insertedRows).toBe(1);
		expect(status).toBe('success');
	}, timeout);
});

describeIf(hasCreds('pubsub'))('PubSub', () => {

	test('pubsub: events', async () => {
		const result = await pubsub(e, 'track', tableNames);
		const { messageId, status } = result;
		expect(Number(messageId)).toBeGreaterThan(0);
		expect(status).toBe('success');
	}, timeout);

	test('pubsub: users', async () => {
		const result = await pubsub(u, 'engage', tableNames);
		const { messageId, status } = result;
		expect(Number(messageId)).toBeGreaterThan(0);
		expect(status).toBe('success');
	}, timeout);

	test('pubsub: groups', async () => {
		const result = await pubsub(g, 'groups', tableNames);
		const { messageId, status } = result;
		expect(Number(messageId)).toBeGreaterThan(0);
		expect(status).toBe('success');
	}, timeout);
});
