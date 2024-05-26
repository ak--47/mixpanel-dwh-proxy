module.exports = function validate(PARAMS = { ...process.env }) {
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
	// ALL
	const {  WAREHOUSES = "", EVENTS_TABLE_NAME, USERS_TABLE_NAME, GROUPS_TABLE_NAME } = PARAMS;
	const DESTINATIONS = WAREHOUSES.split(',').map(wh => wh.trim()).filter(a => a).map(t => t.toLowerCase());	
	if (!EVENTS_TABLE_NAME) errors.push(new Error('EVENTS_TABLE_NAME is required'));
	if (!USERS_TABLE_NAME) errors.push(new Error('USERS_TABLE_NAME is required'));
	if (!GROUPS_TABLE_NAME) errors.push(new Error('GROUPS_TABLE_NAME is required'));
	if (DESTINATIONS.length === 0) errors.push(new Error('DESTINATIONS is required; leave blank to just use mixpanel'));


	// BIGQUERY
	const { bigquery_project = "", bigquery_dataset = "", bigquery_service_account = "", bigquery_service_account_pass = "", bigquery_keyfile = "" } = PARAMS;
	// SNOWFLAKE
	const { snowflake_account = "", snowflake_user = "", snowflake_password = "", snowflake_database = "", snowflake_schema = "", snowflake_warehouse = "", snowflake_role = "", snowflake_access_url = "", snowflake_stage = "", snowflake_pipe = "" } = PARAMS;
	// REDSHIFT
	const { redshift_workgroup = "", redshift_database = "", redshift_access_key_id = "", redshift_secret_access_key = "", redshift_session_token = "", redshift_region = "", redshift_schema_name = "" } = PARAMS;
	// MIXPANEL
	const { mixpanel_token = "" } = PARAMS;

	// bigquery
	if (DESTINATIONS.includes('BIGQUERY')) {
		if (!bigquery_project) errors.push(new Error('bigquery_project is required'));
		if (!bigquery_dataset) errors.push(new Error('bigquery_dataset is required'));
		if (!bigquery_keyfile && (!bigquery_service_account || !bigquery_service_account_pass)) errors.push(new Error('bigquery_keyfile or bigquery_service_account + bigquery_service_account_pass is required'));
	}

	// snowflake
	if (DESTINATIONS.includes('SNOWFLAKE')) {
		if (!snowflake_account) errors.push(new Error('snowflake_account is required'));
		if (!snowflake_user) errors.push(new Error('snowflake_user is required'));
		if (!snowflake_password) errors.push(new Error('snowflake_password is required'));
		if (!snowflake_database) errors.push(new Error('snowflake_database is required'));
		if (!snowflake_schema) errors.push(new Error('snowflake_schema is required'));
		if (!snowflake_warehouse) errors.push(new Error('snowflake_warehouse is required'));
		if (!snowflake_role) errors.push(new Error('snowflake_role is required'));
		if (!snowflake_access_url) errors.push(new Error('snowflake_access_url is required'));
		if (snowflake_stage) {
			if (!snowflake_pipe) errors.push(new Error('snowflake_pipe isis required'));
		}
		if (snowflake_pipe) {
			if (!snowflake_stage) errors.push(new Error('snowflake_stage is required'));
		}
	}

	// redshift
	if (DESTINATIONS.includes('REDSHIFT')) {
		if (!redshift_workgroup) errors.push(new Error('redshift_workgroup is required'));
		if (!redshift_database) errors.push(new Error('redshift_database is required'));
		if (!redshift_access_key_id) errors.push(new Error('redshift_access_key_id is required'));
		if (!redshift_secret_access_key) errors.push(new Error('redshift_secret_access_key is required'));
		if (!redshift_region) errors.push(new Error('redshift_region is required'));
		if (!redshift_schema_name) errors.push(new Error('redshift_schema_name is required'));
	}

	if (DESTINATIONS.includes('MIXPANEL')) {
		if (!mixpanel_token) { } //todo ... do we care?
	}

	if (errors.length) {
		errors.forEach(error => console.error(error.message));
		// throw the first error
		throw new Error(errors.shift()?.message || "unknown error");

	}

	// now sent env vars for each param
	for (const key in PARAMS) {
		process.env[key] = PARAMS[key];
	}

	return PARAMS;
};