// @ts-nocheck
// route-level tests against app.js (no network, no cloud creds)

// must be set BEFORE requiring app: dotenv uses {override: false} so a
// pre-existing env var beats whatever is in .env
process.env.DESTINATIONS = 'MIXPANEL';

// mock the registry so /track exercises parser → fan-out → response shape
// without hitting mixpanel's API
jest.mock('../middleware', () => {
	const mixpanel = Object.assign(
		jest.fn(async (data) => ({ status: 'success', insertedRows: data.length, failedRows: 0 })),
		{ init: jest.fn(), drop: jest.fn(), requiredEnvVars: [] }
	);
	return { mixpanel };
});

const request = require('supertest');
const registry = require('../middleware');
const { app } = require('../app');

describe('ROUTES', () => {

	test('GET /ping', async () => {
		const res = await request(app).get('/ping');
		expect(res.status).toBe(200);
		expect(res.body.status).toBe('OK');
		expect(res.body.message).toBe('pong');
	});

	test('GET /', async () => {
		const res = await request(app).get('/');
		expect(res.status).toBe(200);
		expect(res.body.status).toBe('OK');
	});

	test('/decide is deprecated', async () => {
		const res = await request(app).get('/decide');
		expect(res.status).toBe(299);
	});

	test('POST /track: sendBeacon (form-urlencoded base64)', async () => {
		const event = { event: 'test', properties: { token: 'abc', $device_id: 'qux' } };
		const base64 = Buffer.from(JSON.stringify(event)).toString('base64');
		const res = await request(app)
			.post('/track')
			.set('Content-Type', 'application/x-www-form-urlencoded')
			.send(`data=${encodeURIComponent(base64)}`);

		expect(res.status).toBe(200);
		const mixpanelResult = res.body.find((r) => r.name === 'mixpanel');
		expect(mixpanelResult.result.status).toBe('success');

		// the parsed event made it through the parser to the destination
		const [sentData, sentType] = registry.mixpanel.mock.calls.at(-1);
		expect(sentType).toBe('track');
		expect(sentData[0].event).toBe('test');
	});

	test('POST /track: plain JSON body', async () => {
		const res = await request(app)
			.post('/track')
			.set('Content-Type', 'application/json')
			.send([{ event: 'json test', properties: { token: 'abc' } }]);

		expect(res.status).toBe(200);
		const [sentData] = registry.mixpanel.mock.calls.at(-1);
		expect(sentData[0].event).toBe('json test');
	});

	test('passthrough layers (/record, /flags, /lib) are registered before body parsers', () => {
		const stack = app._router.stack;
		const layerIndex = (path) => stack.findIndex((l) => l.regexp?.test(path) && l.name !== 'router');
		const parserIndex = stack.findIndex((l) => l.name === 'jsonParser');

		expect(parserIndex).toBeGreaterThan(-1);
		for (const path of ['/record', '/flags', '/lib.min.js', '/lib.js']) {
			const idx = layerIndex(path);
			expect(idx).toBeGreaterThan(-1);
			expect(idx).toBeLessThan(parserIndex);
		}
	});

});
