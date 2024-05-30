/* cSpell:disable */
//@ts-nocheck
const { parseSDKData } = require('../components/transforms');
const validate = require('../components/validate');

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


describe('VALIDATION', () => {
	let originalEnv;

	beforeEach(() => {
		// Save the original process.env
		originalEnv = { ...process.env };
	});

	afterEach(() => {
		// Restore the original process.env
		process.env = originalEnv;
	});

	test('default table names', () => {
		process.env = {};
		const params = validate();

		expect(params.EVENTS_TABLE_NAME).toBe('events');
		expect(params.USERS_TABLE_NAME).toBe('users');
		expect(params.GROUPS_TABLE_NAME).toBe('groups');
	});

	test('bigquery: project required', () => {
		process.env = {
			WAREHOUSES: 'BIGQUERY'
		};

		expect(() => validate()).toThrow('bigquery_project is required');
	});

	test('snowflake: account required', () => {
		process.env = {
			WAREHOUSES: 'SNOWFLAKE'
		};

		expect(() => validate()).toThrow('snowflake_account is required');
	});

	test('redshift: workgroup required', () => {
		process.env = {
			WAREHOUSES: 'REDSHIFT'
		};

		expect(() => validate()).toThrow('redshift_workgroup is required');
	});

	test('gcs: project required', () => {
		process.env = {
			LAKES: 'GCS'
		};

		expect(() => validate()).toThrow('gcs_project is required');
	});

	test('s3: bucket required', () => {
		process.env = {
			LAKES: 'S3'
		};

		expect(() => validate()).toThrow('s3_bucket is required');
	});

	test('azure: account required', () => {
		process.env = {
			LAKES: 'AZURE'
		};

		expect(() => validate()).toThrow('azure_account_name is required');
	});

	test('case insensitivity', () => {
		process.env = {
			EVENTS_TABLE_NAME: 'Events',
			USERS_TABLE_NAME: 'Users',
			GROUPS_TABLE_NAME: 'Groups',
			EVENTS_PATH: 'EventsPath',
			USERS_PATH: 'UsersPath',
			GROUPS_PATH: 'GroupsPath'
		};

		const params = validate();

		expect(process.env.events_table_name).toBe('Events');
		expect(process.env.users_table_name).toBe('Users');
		expect(process.env.groups_table_name).toBe('Groups');
		expect(process.env.events_path).toBe('EventsPath');
		expect(process.env.users_path).toBe('UsersPath');
		expect(process.env.groups_path).toBe('GroupsPath');
	});

	test('mixpanel: token optional', () => {
		process.env = {
			WAREHOUSES: 'MIXPANEL'
		};

		expect(() => validate()).not.toThrow();
	});

	test('not overwrite defaults', () => {
		process.env = {
			EVENTS_TABLE_NAME: 'custom_events',
			USERS_TABLE_NAME: 'custom_users',
			GROUPS_TABLE_NAME: 'custom_groups',
			EVENTS_PATH: 'custom_events_path',
			USERS_PATH: 'custom_users_path',
			GROUPS_PATH: 'custom_groups_path'
		};

		const params = validate();

		expect(params.EVENTS_TABLE_NAME).toBe('custom_events');
		expect(params.USERS_TABLE_NAME).toBe('custom_users');
		expect(params.GROUPS_TABLE_NAME).toBe('custom_groups');
		expect(params.EVENTS_PATH).toBe('custom_events_path');
		expect(params.USERS_PATH).toBe('custom_users_path');
		expect(params.GROUPS_PATH).toBe('custom_groups_path');
	});


	test('multiple destinations', () => {
		process.env = {
			WAREHOUSES: 'BIGQUERY, SNOWFLAKE',
			LAKES: 'GCS, S3',
			bigquery_project: 'foo',
			snowflake_account: 'bar',
			gcs_project: 'baz',
			s3_bucket: 'qux',
			bigquery_dataset: 'dataset',
			bigquery_service_account: 'service_account',
			bigquery_service_account_private_key: 'service_account_private_key',
			gcs_bucket: 'bucket',
			s3_region: 'region',
			s3_access_key_id: 'access_key_id',
			s3_secret_access_key: 'secret_access_key',
			snowflake_user: 'user',
			snowflake_password: 'password',
			snowflake_database: 'database',
			snowflake_schema: 'schema',
			snowflake_warehouse: 'warehouse',
			snowflake_role: 'role',
			snowflake_access_url: 'access_url',
			gcs_service_account: 'service_account',
			gcs_service_account_private_key: 'service_account_private_key'

		};

		const params = validate();

		expect(params.WAREHOUSES).toBe('BIGQUERY, SNOWFLAKE');
		expect(params.LAKES).toBe('GCS, S3');
		expect(process.env.warehouses).toBe('BIGQUERY, SNOWFLAKE');
		expect(process.env.lakes).toBe('GCS, S3');

	});

	test('missing env vars', () => {
		process.env = {
			WAREHOUSES: 'BIGQUERY, SNOWFLAKE, REDSHIFT'
		};

		expect(() => validate()).toThrow('bigquery_project is required');
	});


	test('snowflake: pipe requires stage', () => {
		process.env = {
			WAREHOUSES: 'SNOWFLAKE',
			snowflake_pipe: 'pipe',
			snowflake_user: 'user',
			snowflake_account: 'account',
			snowflake_password: 'password',
			snowflake_database: 'database',
			snowflake_schema: 'schema',
			snowflake_warehouse: 'warehouse',
			snowflake_role: 'role',
			snowflake_access_url: 'access_url',
		};

		expect(() => validate()).toThrow('snowflake_stage is required to use pipelines');
	});

	test('mixpanel: no required vars', () => {
		process.env = {
			WAREHOUSES: 'MIXPANEL'
		};

		expect(() => validate()).not.toThrow();
	});



	test('handle empty case', () => {
		process.env = {
			WAREHOUSES: '',
			LAKES: ''
		};

		const params = validate();

		expect(params.WAREHOUSES).toBe('MIXPANEL');
		expect(params.LAKES).toBe('');
	});



});

afterAll(done => {
	done();
});