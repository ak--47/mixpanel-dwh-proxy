const { isJSONStr, clone } = require('ak-tools');
const { parseISO, isValid } = require('date-fns');
const { existsSync } = require('fs');


function inferType(value) {
	//complex objects
	if (Array.isArray(value)) return 'ARRAY';
	if (typeof value === 'object') return 'OBJECT';
	if (isBoolean(value)) return 'BOOLEAN';
	if (isJSONStr(value)) return inferJSONType(value);
	if (isNumber(value)) return inferNumberType(value);
	if (isValidDate(value)) return inferDateType(value);

	return 'STRING';
}

function isBoolean(value) {
	if (typeof value === 'boolean') return true;
	if (typeof value === 'string') {
		const val = value?.toLowerCase();
		return val === 'true' || val === 'false';
	}
	return false;
}

function isNumber(value) {
	return !isNaN(value) && !isNaN(parseFloat(value));
}

function inferNumberType(value) {
	return value?.toString()?.includes('.') ? 'FLOAT' : 'INT';
}

function inferDateType(value) {

	if (value?.includes('T')) {
		return 'TIMESTAMP';
	}
	if (value?.includes('Z')) {
		return 'TIMESTAMP';
	}

	return 'DATE';
}

function inferJSONType(value) {
	if (value?.startsWith('[') && value?.endsWith(']')) {
		return 'ARRAY';
	}
	if (value?.startsWith('{') && value?.endsWith('}')) {
		return 'OBJECT';
	};
	return 'JSON';
}

function range(a, b, step = 1) {
	step = !step ? 1 : step;
	b = b / step;
	for (var i = a; i <= b; i++) {
		this.push(i * step);
	}
	return this;
};


function isValidDate(value) {
	try {
		return isValid(parseISO(value));
	}
	catch (e) {
		return false;

	}
}


function getUniqueKeys(data) {
	const keysSet = new Set();
	data.forEach(item => {
		Object.keys(item).forEach(key => keysSet.add(key));
	});
	return Array.from(keysSet);
}

/**
 * @param  {any[]} data
 * @return {Schema}
 */
function generateSchema(data) {
	const copyData = clone(data);
	const keys = getUniqueKeys(copyData);
	//assume everything is a string
	let schema = keys.map(key => {
		const template = {
			name: key,
			type: 'STRING'
		};

		try {
			while (!copyData[0][key] && copyData.length) {
				copyData.shift();
			}
			template.type = inferType(copyData[0][key]);

		}
		catch (e) {
			console.error(e);
		}
		return template;
	});

	return schema;
}

/**
 * Prepares and cleans header names according to BigQuery's naming restrictions. can return hashmap or array
 * ? see: https://cloud.google.com/bigquery/docs/schemas#column_names
 * @param {string[]} headers - The array of header names to be cleaned.
 * @param {boolean} [asArray=false] - Whether to return the result as an array of arrays.
 * @returns {(Object|string[][])|{}} If asArray is true, returns an array of arrays, each containing
 * the original header name and the cleaned header name. If false, returns an object mapping
 * original header names to cleaned header names.
 */
function prepHeaders(headers, asArray = false) {
	const headerMap = {};
	const usedNames = new Set();

	headers.forEach((originalName, index) => {
		if (originalName === null) originalName = 'null';
		if (originalName === undefined) originalName = 'undefined';
		if (originalName === '') originalName = 'empty_index_' + index;

		let cleanName = originalName.trim();

		// Replace invalid characters
		cleanName = cleanName.replace(/[^a-zA-Z0-9_]/g, '_');

		// Ensure it starts with a letter or underscore
		if (!/^[a-zA-Z_]/.test(cleanName)) {
			cleanName = '_' + cleanName;
		}

		// Trim to maximum length
		cleanName = cleanName.substring(0, 300);

		// Ensure uniqueness
		let uniqueName = cleanName;
		let suffix = 1;
		while (usedNames.has(uniqueName)) {
			uniqueName = cleanName + '_' + suffix++;
		}
		cleanName = uniqueName;

		// Add to used names set
		usedNames.add(cleanName);

		// Map original name to clean name
		headerMap[originalName] = cleanName;
	});

	if (asArray) {
		const oldNames = Object.keys(headerMap);
		const newNames = Object.values(headerMap);
		const result = oldNames.map((key, i) => [key, newNames[i]]);
		return result;
	}

	return headerMap;
}

/**
 * Cleans a string to ensure it is safe to use as a table name or column header in all major DWH platforms.
 * - Replaces any non-alphanumeric characters with underscores.
 * - Ensures the name doesn't start with a digit or reserved keyword.
 * - Removes trailing underscores.
 * - Collapses multiple underscores to a single one.
 * - Converts to lower case for consistency.
 * - Ensures a minimum length by adding a default prefix if too short.
 * - Limits the length to 300 characters.
 * @param {string} name - The name to clean.
 * @return {string}
 */
function cleanName(name) {
	try {
		const reservedKeywords = new Set(['SELECT', 'TABLE', 'DELETE', 'INSERT', 'UPDATE']); // Example reserved keywords
		name = name
			.replace(/[^a-zA-Z0-9_]+/g, '_') // Replace sequences of non-alphanumeric characters with underscore
			.replace(/^[\d_]+/, '_') // Replace leading digits or underscores
			.replace(/_+$/, '') // Remove trailing underscores
			.replace(/_+/g, '_') // Collapse multiple underscores into one
			.toLowerCase() // Convert to lower case for consistency
			.substring(0, 300); // Limit the length to 300 characters

		// Handle reserved keywords and ensure minimum length
		if (reservedKeywords.has(name.toUpperCase()) || name.length < 3) {
			name = `db_${name}`; // Add a prefix if the name is a reserved keyword or too short
		}

		return name;
	}
	catch (e) {
		return 'db_unknown_' + Math.random().toString(36).substring(5);
	}
}

/**
 * Validates environment variables; returns found and missing values.
 * @param  {string[]} keysToCheck - Array of keys to check in the environment variables.
 * @param  {Object} params - hash of variables to enrich with found values.
 * @returns {Object} An object containing the found values, missing values, and updated variables.
 */
function checkEnv(keysToCheck = [], params = {}) {
	const valsFound = {};
	const valsMissing = [];

	if (typeof keysToCheck === 'string') keysToCheck = [keysToCheck];

	for (const [index, key] of keysToCheck.entries()) {
		const found = process.env[key] || process.env[key?.toUpperCase()] || process.env[key?.toLowerCase()];
		if (found) {
			valsFound[key] = found;
			params[key] = found;
		} else {
			valsMissing.push(key);
		}
	}
	return { valsFound, valsMissing };
}


function validate(PARAMS) {
	for (const key in PARAMS) {
		if (PARAMS[key] === undefined) delete PARAMS[key];
		if (PARAMS[key] === '') delete PARAMS[key];
		if (PARAMS[key] === null) delete PARAMS[key];
	}
	const errors = [];
	const { warehouse = "", table_name = "", csv_file = "", json_file = "", batch_size = "", dry_run = "", verbose = "", write_logs = "" } = PARAMS;
	// BIGQUERY
	const { bigquery_project = "", bigquery_dataset = "", bigquery_service_account = "", bigquery_service_account_pass = "", bigquery_keyfile = "" } = PARAMS;
	// SNOWFLAKE
	const { snowflake_account = "", snowflake_user = "", snowflake_password = "", snowflake_database = "", snowflake_schema = "", snowflake_warehouse = "", snowflake_role = "", snowflake_access_url = "" } = PARAMS;
	// REDSHIFT
	const { redshift_workgroup = "", redshift_database = "", redshift_access_key_id = "", redshift_secret_access_key = "", redshift_session_token = "", redshift_region = "", redshift_schema_name = "" } = PARAMS;

	// everything requires
	if (!warehouse) errors.push(new Error('warehouse is required'));
	if (!Array.isArray(warehouse)) errors.push(new Error('warehouse must be an array'));
	if (!table_name) errors.push(new Error('table_name is required'));
	if (!csv_file && !json_file) errors.push(new Error('csv_file or json_file is required'));
	if (!batch_size) errors.push(new Error('batch_size is required'));
	
	//supplied data
	if (csv_file && json_file) errors.push(new Error('cannot specify both csv_file and json_file'));
	if (!csv_file && !json_file) errors.push(new Error('csv_file or json_file is required'));
	if (csv_file) if (!existsSync(csv_file)) errors.push(new Error(`file not found: ${csv_file}`));
	if (json_file) if (!existsSync(json_file)) errors.push(new Error(`file not found: ${json_file}`));

	// bigquery
	if (warehouse.includes('bigquery')) {
		if (!bigquery_project) errors.push(new Error('bigquery_project is required'));
		if (!bigquery_dataset) errors.push(new Error('bigquery_dataset is required'));
		if (!bigquery_keyfile && (!bigquery_service_account || !bigquery_service_account_pass)) errors.push(new Error('bigquery_keyfile or bigquery_service_account + bigquery_service_account_pass is required'));
	}

	// snowflake
	if (warehouse.includes('snowflake')) {
		if (!snowflake_account) errors.push(new Error('snowflake_account is required'));
		if (!snowflake_user) errors.push(new Error('snowflake_user is required'));
		if (!snowflake_password) errors.push(new Error('snowflake_password is required'));
		if (!snowflake_database) errors.push(new Error('snowflake_database is required'));
		if (!snowflake_schema) errors.push(new Error('snowflake_schema is required'));
		if (!snowflake_warehouse) errors.push(new Error('snowflake_warehouse is required'));
		if (!snowflake_role) errors.push(new Error('snowflake_role is required'));
		if (!snowflake_access_url) errors.push(new Error('snowflake_access_url is required'));
	}

	// redshift
	if (warehouse.includes('redshift')) {
		if (!redshift_workgroup) errors.push(new Error('redshift_workgroup is required'));
		if (!redshift_database) errors.push(new Error('redshift_database is required'));
		if (!redshift_access_key_id) errors.push(new Error('redshift_access_key_id is required'));
		if (!redshift_secret_access_key) errors.push(new Error('redshift_secret_access_key is required'));
		if (!redshift_region) errors.push(new Error('redshift_region is required'));
		if (!redshift_schema_name) errors.push(new Error('redshift_schema_name is required'));
	}

	if (errors.length) {
		errors.forEach(error => console.error(error.message));
		// throw the first error
		throw new Error(errors.shift()?.message || "unknown error");

	}

	return PARAMS;
}

module.exports = {
	inferType,
	range,
	getUniqueKeys,
	inferJSONType,
	inferNumberType,
	isNumber,
	isBoolean,
	isJSONStr,
	generateSchema,
	prepHeaders,
	cleanName,
	checkEnv,
	validate
};