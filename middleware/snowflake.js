/*
----
SNOWFLAKE MIDDLEWARE
----
*/

const snowflake = require('snowflake-sdk');
const u = require('ak-tools');
const { prepHeaders, cleanName, checkEnv } = require('../components/inference');
const log = require('../components/logger.js');
require('dotenv').config();


/**
 * Main Function: Inserts data into Snowflake
 * @param {import('../types.js').Schema} schema
 * @param {import('../types.js').csvBatch[]} batches
 * @param {import('../types.js').JobConfig} PARAMS
 * @returns {Promise<import('../types.js').WarehouseUploadResult>}
 */
async function loadToSnowflake(schema, batches, PARAMS) {
	let {
		snowflake_account = '',
		snowflake_user = '',
		snowflake_password = '',
		snowflake_database = '',
		snowflake_schema = '',
		snowflake_warehouse = '',
		snowflake_role = '',
		snowflake_access_url = '',
		dry_run = false,
		verbose = false,
		table_name = ''
	} = PARAMS;

	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!table_name) throw new Error('table_name is required');

	const conn = await createSnowflakeConnection(PARAMS);
	const isConnectionValid = await conn.isValidAsync();
	if (typeof isConnectionValid === 'boolean' && !isConnectionValid) throw new Error(`Invalid Snowflake credentials`);

	table_name = cleanName(table_name);
	// @ts-ignore
	schema = schema.map(prepareSnowflakeSchema);
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	const snowflake_table_schema = schemaToSnowflakeSQL(schema);
	/** @type {import('../types.js').InsertResult[]} */
	const upload = [];

	let createTableSQL;
	try {
		// Create or replace table
		createTableSQL = `CREATE OR REPLACE TABLE ${table_name} (${snowflake_table_schema})`;
		const tableCreation = await executeSQL(conn, createTableSQL);
		log(`Table ${table_name} created (or replaced) successfully`);
	}
	catch (error) {
		log(`Error creating table; ${error.message}`, error, { createTableSQL });
		debugger;
	}


	log('\n\n');

	let currentBatch = 0;
	if (dry_run) {
		log('Dry run requested. Skipping Snowflake upload.');
		return { schema, database: snowflake_database, table: table_name || "no table", upload: [], logs: log.getLog() };
	}
	// Insert data
	for (const batch of batches) {
		currentBatch++;
		const [insertSQL, hasVariant] = prepareInsertSQL(schema, table_name);
		let data;
		if (hasVariant) {
			//variant columns need to be stringified as an ENTIRE ROW
			//this is weird
			data = [batch.map(row => prepareComplexRows(row, schema))].map(rows => JSON.stringify(rows));
		}
		else {
			//datasets without variant columns can be inserted as an array of arrays (flatMap)
			data = batch.map(row => schema.map(f => formatBindValue(row[f.name], f.type))); //.map(row => JSON.stringify(row));
		}
		const start = Date.now();
		try {
			const task = await executeSQL(conn, insertSQL, data);
			const duration = Date.now() - start;
			const insertedRows = task?.[0]?.['number of rows inserted'] || 0;
			const failedRows = batch.length - insertedRows;
			upload.push({ duration, status: 'success', insertedRows, failedRows });
			if (log.isVerbose()) u.progress(`\tsent batch ${u.comma(currentBatch)} of ${u.comma(batches.length)} batches`);
		} catch (error) {
			const duration = Date.now() - start;
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			log(`Error inserting batch ${currentBatch}: ${error.message}`, error, batch);
		}
	}
	log('\n\nData insertion complete; Terminating Connection...\n');

	const logs = log.getLog();
	/** @type {import('../types.js').WarehouseUploadResult} */
	const uploadJob = { schema, database: snowflake_database, table: table_name || "", upload, logs };



	//conn.destroy(terminationHandler);
	// @ts-ignore
	conn.destroy();


	return uploadJob;

}


/**
 * @param  {import('../types.js').SchemaField} field
 */
function prepareSnowflakeSchema(field) {
	const { name, type } = field;
	let snowflakeType = type.toUpperCase();
	switch (type) {
		case 'INT': snowflakeType = 'NUMBER'; break;
		case 'FLOAT': snowflakeType = 'FLOAT'; break;
		case 'STRING': snowflakeType = 'VARCHAR'; break;
		case 'BOOLEAN': snowflakeType = 'BOOLEAN'; break;
		case 'DATE': snowflakeType = 'DATE'; break;
		case 'TIMESTAMP': snowflakeType = 'TIMESTAMP'; break;
		case 'JSON': snowflakeType = 'VARIANT'; break;
		case 'ARRAY': snowflakeType = 'VARIANT'; break;
		case 'OBJECT': snowflakeType = 'VARIANT'; break;
		// Add other type mappings as necessary
	}
	return { name, type: snowflakeType };
}

/**
 * re-parse complex columns from JSON string to JSON object;
 * needed because of https://github.com/snowflakedb/snowflake-connector-nodejs/issues/59#issuecomment-1677672298
 * @param  {Object} row
 * @param  {import('../types.js').Schema} schema
 */
function prepareComplexRows(row, schema) {
	const variantCols = schema.filter(f => f.type === 'VARIANT');
	for (const col of variantCols) {
		if (row[col.name]) {
			try {
				if (typeof row[col.name] === 'string') row[col.name] = JSON.parse(row[col.name]);
			}
			catch (e) {
				debugger;
				log(`Error inserting batch ${col.name}; ${e.message}`, e);
			}
		}
	}

	for (const key in row) {
		const value = row[key];
		if (value === null || value === undefined || value === "null" || value === "") {
			row[key] = null; // Convert null-like strings to actual null
		}
	}

	return row;
}

function formatBindValue(value, type) {
	if (value === null || value === undefined || value === "null" || value === "" || value?.toString()?.trim() === "") {
		return null; // Convert null-like strings to actual null
	}
	else if (type === 'VARIANT') {
		return value;
		// return JSON.parse(value); // Return the value directly if it's a JSON object
	}
	else if (typeof value === 'string' && u.isJSONStr(value)) {
		// Check if the string is JSON, parse it to actual JSON
		try {
			const parsed = JSON.parse(value); //todo is this necessary?
			if (Array.isArray(parsed)) {
				// If it's an array, return it as-is so Snowflake interprets it as an array
				return parsed;
			} else {
				// If it's any other kind of JSON, return the parsed JSON
				return parsed;
			}
		} catch (e) {
			// If JSON parsing fails, return the original string (should not happen since you check with isJSONStr)
			return value;
		}
	} else {
		return value; // Return the value directly if not a JSON string
	}
}


/**
 * Creates an appropriate SQL statement for inserting data into a Snowflake table
 * VARIANT types are handled by parsing JSON and flattening the data, primitives use VALUES (?,?,?) 
 * ? https://docs.snowflake.com/en/developer-guide/node-js/nodejs-driver-execute#binding-an-array-for-bulk-insertions
 * ? https://github.com/snowflakedb/snowflake-connector-nodejs/issues/59
 * @param  {import('../types.js').Schema} schema
 * @param  {string} tableName
 * @returns {[string, boolean]}
 */
function prepareInsertSQL(schema, tableName) {
	const hasVariant = schema.some(field => field.type === 'VARIANT');
	if (hasVariant) {
		// Build an SQL statement that uses FLATTEN and PARSE_JSON for VARIANT types
		// Adjust select part to correctly handle case sensitivity and data extraction
		const selectParts = schema.map(field => {
			if (field.type === 'VARIANT') {
				// Assuming JSON keys exactly match the field names in case and spelling
				return `value:${field.name.toLowerCase()} AS ${field.name}`;
			} else {
				// Directly use the field name for non-VARIANT columns
				return `value:${field.name.toLowerCase()} AS ${field.name}`;
			}
		}).join(", ");

		// The query assumes that the JSON object keys match the lowercase version of the column names
		return [`
            INSERT INTO ${tableName}
            SELECT ${selectParts}
            FROM TABLE(FLATTEN(PARSE_JSON(?)))
        `, true];
	} else {
		// Regular insert without JSON parsing
		const columnNames = schema.map(f => f.name).join(", ");
		const placeholders = schema.map(() => '?').join(", ");
		return [`INSERT INTO ${tableName} (${columnNames}) VALUES (${placeholders})`, false];
	}
}

/**
 * Translates a schema definition to Snowflake SQL data types
 * @param {import('../types.js').Schema} schema 
 * @returns {string}
 */
function schemaToSnowflakeSQL(schema) {
	return schema.map(field => {
		let type = field.type.toUpperCase();
		switch (type) {
			case 'INT': type = 'NUMBER'; break;
			case 'FLOAT': type = 'FLOAT'; break;
			case 'STRING': type = 'VARCHAR'; break;
			case 'BOOLEAN': type = 'BOOLEAN'; break;
			case 'DATE': type = 'DATE'; break;
			case 'TIMESTAMP': type = 'TIMESTAMP'; break;
			case 'JSON': type = 'VARIANT'; break;
			// Add other type mappings as necessary
		}
		return `${field.name} ${type}`;
	}).join(', ');
}

/**
 * Executes a given SQL query on the Snowflake connection
 * ? https://docs.snowflake.com/en/developer-guide/node-js/nodejs-driver-execute#binding-an-array-for-bulk-insertions
 * @param {snowflake.Connection} conn 
 * @param {string} sql 
 * @param {snowflake.Binds} [binds] pass binds to bulk insert
 * @returns {Promise<snowflake.StatementStatus | any[] | undefined>}
 */
function executeSQL(conn, sql, binds) {
	return new Promise((resolve, reject) => {


		const options = { sqlText: sql };
		if (binds) options.binds = binds;
		if (binds) options.parameters = { MULTI_STATEMENT_COUNT: 1 };

		conn.execute({
			...options,
			complete: (err, stmt, rows) => {
				if (err) {
					log(`Failed executing SQL: ${err.message}`, err, options);
					reject(err);
				} else {
					resolve(rows);
				}
			}
		});
	});


}

/**
 * Establishes a connection to Snowflake
 * @param {import('../types.js').JobConfig} PARAMS 
 * @returns {Promise<snowflake.Connection>}
 */
async function createSnowflakeConnection(PARAMS) {
	log('Attempting to connect to Snowflake...');
	const { snowflake_account = "",
		snowflake_user = "",
		snowflake_password = "",
		snowflake_database = "",
		snowflake_schema = "",
		snowflake_warehouse = "",
		snowflake_role = "",
		snowflake_access_url = ""
	} = PARAMS;
	if (!snowflake_account) throw new Error('snowflake_account is required');
	if (!snowflake_user) throw new Error('snowflake_user is required');
	if (!snowflake_password) throw new Error('snowflake_password is required');
	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!snowflake_schema) throw new Error('snowflake_schema is required');
	if (!snowflake_warehouse) throw new Error('snowflake_warehouse is required');
	if (!snowflake_role) throw new Error('snowflake_role is required');

	//todo: dev logging configuration
	snowflake.configure({ keepAlive: true, logLevel: 'WARN' });


	return new Promise((resolve, reject) => {
		const connection = snowflake.createConnection({
			account: snowflake_account,
			username: snowflake_user,
			password: snowflake_password,
			database: snowflake_database,
			schema: snowflake_schema,
			warehouse: snowflake_warehouse,
			role: snowflake_role,
			accessUrl: snowflake_access_url,
		});


		connection.connect((err, conn) => {
			if (err) {
				log('Failed to connect to Snowflake:', err);
				debugger;
				reject(err);
			} else {
				log('Successfully connected to Snowflake');
				resolve(conn);
			}
		});
	});
}


function terminationHandler(err, conn) {
	if (err) {
		console.error('Unable to disconnect: ' + err.message);
	} else {
		log('\tDisconnected connection with id: ' + conn.getId());
	}
}

/**
 * Properly formats a value for SQL statement.
 * @param {any} value The value to be formatted.
 * @returns {string} The formatted value for SQL.
 */
function formatSQLValue(value) {
	if (value === null || value === "" || value === undefined || value === "null") {
		return 'NULL';
	} else if (u.isJSONStr(value)) {
		const parsed = JSON.parse(value);
		if (Array.isArray(parsed)) {
			const formattedArray = parsed.map(item =>
				item === null ? 'NULL' : (typeof item === 'string' ? `'${item.replace(/'/g, "''")}'` : item)
			);
			return `ARRAY_CONSTRUCT(${formattedArray.join(", ")})`;
		}
		return `PARSE_JSON('${value.replace(/'/g, "''")}')`;
	} else if (typeof value === 'string') {
		return `'${value.replace(/'/g, "''")}'`;
	} else {
		return value;
	}
}

module.exports = loadToSnowflake;
