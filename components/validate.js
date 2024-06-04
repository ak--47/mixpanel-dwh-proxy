/**
 * @fileoverview validates incoming environment variables
 */


/** @typedef {import('../types').Targets} Targets */

const defaultEventsTableName = 'events';
const defaultUsersTableName = 'users';
const defaultGroupsTableName = 'groups';
const defaultWarehouse = 'MIXPANEL';

function validate(PARAMS = { ...process.env }) {

	for (const key in PARAMS) {
		if (PARAMS[key] === undefined) delete PARAMS[key];
		else if (PARAMS[key] === '') delete PARAMS[key];
		else if (PARAMS[key] === null) delete PARAMS[key];
		else {
			// case insensitive
			PARAMS[key?.toLowerCase()] = PARAMS[key];
			PARAMS[key?.toUpperCase()] = PARAMS[key];
			PARAMS[key] = PARAMS[key];
		}
	}
	const errors = [];
	let { DESTINATIONS = "",
		EVENTS_TABLE_NAME,
		USERS_TABLE_NAME,
		GROUPS_TABLE_NAME,
	} = PARAMS;

	/** @type {Targets[]} */
	const TARGETS = DESTINATIONS.split(',').map(wh => wh.trim().toUpperCase()).filter(a => a);
	// @ts-ignore
	PARAMS.TARGETS = TARGETS;

	if (!EVENTS_TABLE_NAME) {
		PARAMS.EVENTS_TABLE_NAME = defaultEventsTableName;
		EVENTS_TABLE_NAME = defaultEventsTableName;
		process.env.EVENTS_TABLE_NAME = defaultEventsTableName;
	}
	if (!USERS_TABLE_NAME) {
		PARAMS.USERS_TABLE_NAME = defaultUsersTableName;
		USERS_TABLE_NAME = defaultUsersTableName;
		process.env.USERS_TABLE_NAME = defaultUsersTableName;
	}
	if (!GROUPS_TABLE_NAME) {
		PARAMS.GROUPS_TABLE_NAME = defaultGroupsTableName;
		GROUPS_TABLE_NAME = defaultGroupsTableName;
		process.env.GROUPS_TABLE_NAME = defaultGroupsTableName;
	}

	if (TARGETS.length === 0) {
		TARGETS.push(defaultWarehouse);
	}

	// BIGQUERY
	const {
		bigquery_project = "",
		bigquery_dataset = "",
		bigquery_service_account = "",
		bigquery_service_account_private_key = "",
		bigquery_keyfile = ""
	} = PARAMS;
	
	// SNOWFLAKE
	const {
		snowflake_account = "",
		snowflake_user = "",
		snowflake_password = "",
		snowflake_database = "",
		snowflake_schema = "",
		snowflake_warehouse = "",
		snowflake_role = "",
		snowflake_access_url = "",
		snowflake_stage = "",
		snowflake_pipe = "",
		snowflake_private_key = "",
		snowflake_region = "",
		snowflake_provider = "",
		snowflake_task = "",
	} = PARAMS;

	let {
		snowflake_task_schedule = 3,
	} = PARAMS;
	
	// REDSHIFT
	const {
		redshift_workgroup = "",
		redshift_database = "",
		redshift_access_key_id = "",
		redshift_secret_access_key = "",
		redshift_session_token = "",
		redshift_region = "",
		redshift_schema_name = ""
	} = PARAMS;
	
	// MIXPANEL
	const {
		mixpanel_token = ""
	} = PARAMS;
	
	// GCS
	const {
		gcs_project = "",
		gcs_bucket = "",
		gcs_service_account = "",
		gcs_service_account_private_key = "",
		gcs_keyfile = ""
	} = PARAMS;
	
	// S3
	const {
		s3_bucket = "",
		s3_region = "",
		s3_access_key_id = "",
		s3_secret_access_key = ""
	} = PARAMS;
	
	// AZURE BLOB STORAGE
	const {
		azure_account = "",
		azure_key = "",
		azure_container = "",
		azure_connection_string = ""
	} = PARAMS;

	// bigquery
	if (TARGETS.includes('BIGQUERY')) {
		if (!bigquery_project) errors.push(new Error('bigquery_project is required'));
		if (!bigquery_dataset) errors.push(new Error('bigquery_dataset is required'));
		// if (!bigquery_keyfile && (!bigquery_service_account || !bigquery_service_account_private_key)) errors.push(new Error('bigquery_keyfile or bigquery_service_account + bigquery_service_account_private_key is required'));
	}

	// snowflake
	if (TARGETS.includes('SNOWFLAKE')) {
		if (!snowflake_account) errors.push(new Error('snowflake_account is required'));
		if (!snowflake_user) errors.push(new Error('snowflake_user is required'));
		if (!snowflake_password) errors.push(new Error('snowflake_password is required'));
		if (!snowflake_database) errors.push(new Error('snowflake_database is required'));
		if (!snowflake_schema) errors.push(new Error('snowflake_schema is required'));
		if (!snowflake_warehouse) errors.push(new Error('snowflake_warehouse is required'));
		if (!snowflake_role) errors.push(new Error('snowflake_role is required'));
		if (!snowflake_access_url) errors.push(new Error('snowflake_access_url is required'));
		if (snowflake_pipe && !snowflake_stage) errors.push(new Error('snowflake_stage is required to use pipelines'));
		if (snowflake_pipe) {
			if (!snowflake_private_key) errors.push(new Error('snowflake_private_key is required'));
			if (!snowflake_region) errors.push(new Error('snowflake_region is required'));
		}

		if (snowflake_task) {
			if (!snowflake_task_schedule) errors.push(new Error('snowflake_task_schedule is required'));
			if (typeof snowflake_task_schedule !== 'number') {
				try {
					snowflake_task_schedule = parseInt(snowflake_task_schedule);
				}
				catch (e) {
					errors.push(new Error('snowflake_task_schedule must be a number'));
				}
			}
		}

		// if (snowflake_pipe) {
		// 	if (!snowflake_stage) errors.push(new Error('snowflake_stage is required to use pipelines'));
		// }
	}

	// redshift
	if (TARGETS.includes('REDSHIFT')) {
		if (!redshift_workgroup) errors.push(new Error('redshift_workgroup is required'));
		if (!redshift_database) errors.push(new Error('redshift_database is required'));
		if (!redshift_access_key_id) errors.push(new Error('redshift_access_key_id is required'));
		if (!redshift_secret_access_key) errors.push(new Error('redshift_secret_access_key is required'));
		if (!redshift_region) errors.push(new Error('redshift_region is required'));
		if (!redshift_schema_name) errors.push(new Error('redshift_schema_name is required'));
	}

	if (TARGETS.includes('MIXPANEL')) {
		if (!mixpanel_token) {
			//we don't actually care about this
		}
	}

	if (TARGETS.includes('GCS')) {
		if (!gcs_project) errors.push(new Error('gcs_project is required'));
		if (!gcs_bucket) errors.push(new Error('gcs_bucket is required'));

	}

	if (TARGETS.includes('S3')) {
		if (!s3_bucket) errors.push(new Error('s3_bucket is required'));
		if (!s3_region) errors.push(new Error('s3_region is required'));
		if (!s3_access_key_id) errors.push(new Error('s3_access_key_id is required'));
		if (!s3_secret_access_key) errors.push(new Error('s3_secret_access_key is required'));
	}

	if (TARGETS.includes('AZURE')) {
		if (!azure_account) errors.push(new Error('azure_account is required'));
		if (!azure_connection_string && !azure_key) errors.push(new Error('azure_key or azure_connection_string is required'));
		if (!azure_container) errors.push(new Error('azure_container is required'));
	}

	if (errors.length) {
		errors.forEach(error => console.error(error.message));
		// throw the first error
		throw new Error(errors.shift()?.message || "unknown error");

	}

	// now set env vars for each param, case insensitive
	for (const key in PARAMS) {
		process.env[key?.toLowerCase()] = PARAMS[key];
		process.env[key?.toUpperCase()] = PARAMS[key];
		process.env[key] = PARAMS[key];
	}

	return PARAMS;
};


module.exports = validate;