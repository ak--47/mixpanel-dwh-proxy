/* cSpell:disable */
//@ts-nocheck
const { parseSDKData } = require('../components/parser');

describe('PARSING', () => {
	console.error = jest.fn();
	test('{}', () => {
		const input = JSON.stringify({ key: 'value' });
		expect(parseSDKData(input)).toEqual([{ key: 'value' }]);
	});

	test('[{}, {}, {}]', () => {
		const input = JSON.stringify([{ key: 'value' }, { key: 'value' }, { key: 'value' }]);
		expect(parseSDKData(input)).toEqual([{ key: 'value' }, { key: 'value' }, { key: 'value' }]);
	});

	test('base64 encoded', () => {
		const json = JSON.stringify({ key: 'value' });
		const base64 = Buffer.from(json).toString('base64');
		expect(parseSDKData(base64)).toEqual([{ key: 'value' }]);
	});

	test('sendBeacon', () => {
		const json = JSON.stringify({ key: 'value' });
		const encoded = encodeURIComponent(Buffer.from(json).toString('base64'));
		const input = `data=${encoded}`;
		expect(parseSDKData(input)).toEqual([{ key: 'value' }]);
	});

	test('unknown format', () => {
		const input = 'definitely not jason';
		expect(parseSDKData(input)).toEqual([]);
	});
});


afterAll(done => {
	done();
});