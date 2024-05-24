/*
----
REDSHIFT MIDDLEWARE
----
*/
const { RedshiftDataClient, ExecuteStatementCommand, GetStatementResultCommand, DescribeStatementCommand } = require('@aws-sdk/client-redshift-data');
const u = require('ak-tools');
const { prepHeaders, cleanName, checkEnv } = require('../components/inference');
const log = require('../components/logger.js');
require('dotenv').config();

let workgroup;
let database;
let region;


/**
 * Main Function: Inserts data into Redshift Serverless
 * @param {import('../types').Schema} schema
 * @param {import('../types').csvBatch[]} batches
 * @param {import('../types').JobConfig} PARAMS
 * @returns {Promise<import('../types').WarehouseUploadResult>}
 */
async function loadToRedshift(schema, batches, PARAMS) {
	let {
		redshift_workgroup,
		redshift_database,
		table_name,
		dry_run,
		redshift_access_key_id,
		redshift_secret_access_key,
		redshift_session_token,
		redshift_region,
		redshift_schema_name = "public"
	} = PARAMS;

	//ensure we have everything we need
	if (!redshift_database) throw new Error('redshift_database is required');
	if (!redshift_workgroup) throw new Error('redshift_workgroup is required');
	if (!table_name) throw new Error('table_name is required');
	if (!redshift_schema_name) throw new Error('redshift_schema_name is required');
	if (!redshift_access_key_id) throw new Error('redshift_access_key_id is required');
	if (!redshift_secret_access_key) throw new Error('redshift_secret_access_key is required');
	if (!redshift_region) throw new Error('redshift_region is required');

	// Set the global variables
	database = redshift_database;
	workgroup = redshift_workgroup;
	region = redshift_region;

	const credentials = {
		accessKeyId: redshift_access_key_id,
		secretAccessKey: redshift_secret_access_key,
	};
	if (redshift_session_token) credentials.sessionToken = redshift_session_token;

	table_name = cleanName(table_name);

	schema = schema.map(field => generalSchemaToRedshiftSchema(field));
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	/** @type {import('@aws-sdk/client-redshift-data').RedshiftDataClientConfig} */
	const clientConfig = { region: region, credentials };
	const redshiftClient = new RedshiftDataClient(clientConfig);
	const tableSchemaSQL = schemaToRedshiftSQL(schema);

	const validCredentials = await verifyRedshiftCredentials(redshiftClient);
	if (typeof validCredentials === 'boolean' && !validCredentials) throw new Error(`Invalid BigQuery credentials; Got Message: ${validCredentials}`);

	/** @type {import('../types').InsertResult[]} */
	const upload = [];


	try {
		// const createTableSQL = `CREATE TABLE IF NOT EXISTS ${redshift_schema_name}.${table_name} (${tableSchemaSQL})`;
		const dropAndCreateTableSQL = `
    DROP TABLE IF EXISTS ${redshift_schema_name}.${table_name};
    CREATE TABLE ${redshift_schema_name}.${table_name} (${tableSchemaSQL});
`.trim();
		const tableCreate = await executeSQL(redshiftClient, dropAndCreateTableSQL);
		log(`Table ${table_name} created or verified successfully`);
	} catch (error) {
		console.error(`Error creating table; ${error.message}`);
		debugger;
	}

	log('\n\n');

	if (dry_run) {
		log('Dry run requested. Skipping Redshift upload.');
		return { schema, database: redshift_database, table: table_name || "", upload: [], logs: log.getLog() };
	}

	//insert statements
	const columnNames = schema.map(f => f.name).join(", ");

	// Process each batch
	for (const batch of batches) {
		let valuesArray = [];
		for (const row of batch) {
			let rowValues = schema.map(field => {
				let value = row[field.name];
				return formatSQLValue(value, field.type);
			}).join(", ");
			valuesArray.push(`(${rowValues})`);
		}
		const valuesString = valuesArray.join(", ");
		const insertSQL = `INSERT INTO ${table_name} (${columnNames}) VALUES ${valuesString}`;

		// Execute the batch insert
		const start = Date.now();
		try {
			const response = await executeSQL(redshiftClient, insertSQL, true);
			const duration = Date.now() - start;
			const insertedRows = response || 0; // If response is null, assume 0 rows inserted
			const failedRows = batch.length - insertedRows;
			upload.push({ duration, status: 'success', insertedRows, failedRows });
			if (log.isVerbose()) u.progress(`\tsent batch ${batches.indexOf(batch) + 1} of ${u.comma(batches.length)} batches`);
		} catch (error) {
			const duration = Date.now() - start;
			upload.push({ status: 'error', errorMessage: error.message, errors: error, duration });
			log(`Error inserting batch ${batches.indexOf(batch) + 1}: ${error.message}`, error, batch);
			debugger;
		}
	}

	log('\n\nData insertion complete.\n');

	const logs = log.getLog();
	/** @type {import('../types').WarehouseUploadResult} */
	const uploadJob = { schema, database: redshift_database, table: table_name || "", upload, logs };

	return uploadJob;
}


/**
 * Executes a given SQL query on the Redshift Serverless connection
 * @param {RedshiftDataClient} client 
 * @param {string} sql 
 * @param {boolean} isBatch
 * @returns {Promise<number | null>}
 */
async function executeSQL(client, sql, isBatch = false) {
	const executeCommand = new ExecuteStatementCommand({
		Sql: sql,
		Database: database,
		WorkgroupName: workgroup,
	});

	try {
		const statement = await client.send(executeCommand);
		if (!isBatch) return null;
		// Wait for the statement to complete
		const statementId = statement.Id;
		const describeCommand = new DescribeStatementCommand({ Id: statementId });
		let statementStatus;
		let describeResponse;
		do {
			describeResponse = await client.send(describeCommand);
			statementStatus = describeResponse.Status;
			if (statementStatus === 'FAILED' || statementStatus === 'ABORTED') {
				throw new Error(`Statement ${statementStatus}: ${describeResponse.Error}`);
			}
			// Wait for a while before checking the status again
			if (statementStatus !== 'FINISHED') {
				const waitTime = u.rand(250, 420);
				log(`Statement ${statementId} is ${statementStatus}. Waiting ${waitTime}ms before checking again...`);
				await u.sleep(waitTime);
			}
		} while (statementStatus !== 'FINISHED');
		//query is done;
		const { ResultRows = null } = describeResponse;
		return ResultRows;

	} catch (error) {
		debugger;
		console.error('Failed executing SQL:', error);
		throw error;
	}
}


function formatSQLValue(value, type) {
	if (value === null || value === undefined || value === "") return 'NULL';
	switch (type) {
		case 'INTEGER':
			return parseInt(value, 10);
		case 'REAL':
			return parseFloat(value);
		case 'BOOLEAN':
			return value.toString().toLowerCase() === 'true' ? 'TRUE' : 'FALSE'; // Ensure boolean conversion
		case 'STRING':
			return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
		case 'VARCHAR':
			return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
		case 'DATE':
			return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
		case 'TIMESTAMP':
			return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
		case 'SUPER': // For JSON types
			if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
			if (typeof value === 'object') return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
			if (Array.isArray(value)) return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
			//never should get here
			debugger;
		default:
			return value;
	}
}

/**
 * Translates a schema definition to Redshift SQL data types
 * @param {import('../types').Schema} schema 
 * @returns {string}
 */
function schemaToRedshiftSQL(schema) {
	return schema.map(field => {
		let type = field.type.toUpperCase();
		switch (type) {
			case 'INT': type = 'INTEGER'; break;
			case 'FLOAT': type = 'REAL'; break;
			case 'STRING': type = 'VARCHAR'; break;
			case 'BOOLEAN': type = 'BOOLEAN'; break;
			case 'DATE': type = 'DATE'; break;
			case 'TIMESTAMP': type = 'TIMESTAMP'; break;
			case 'JSON': type = 'SUPER'; break;
			case 'OBJECT': type = 'SUPER'; break;
			case 'ARRAY': type = 'SUPER'; break;
			// Add other type mappings as necessary
		}
		return `${field.name} ${type}`;
	}).join(', ');
}

/**
 * Prepares the schema for Redshift based on the field types provided.
 * @param {import('../types').SchemaField} field
 * @returns {import('../types').SchemaField}
 */
function generalSchemaToRedshiftSchema(field) {
	const redshiftTypes = {
		'INT': 'INTEGER',
		'FLOAT': 'REAL',
		'STRING': 'VARCHAR',
		'BOOLEAN': 'BOOLEAN',
		'DATE': 'DATE',
		'TIMESTAMP': 'TIMESTAMP',
		'JSON': 'SUPER',
		'OBJECT': 'SUPER',
		'ARRAY': 'SUPER'
	};

	let redshiftType = redshiftTypes[field.type.toUpperCase()] || 'VARCHAR';  // Default to VARCHAR if not mapped
	return { ...field, type: redshiftType };
}

/**
 * @param  {RedshiftDataClient} redshiftClient
 */
async function verifyRedshiftCredentials(redshiftClient) {
	const sql = 'SELECT 1';
	const command = new ExecuteStatementCommand({
		Sql: sql,
		Database: database,
		WorkgroupName: workgroup

	});

	try {
		await redshiftClient.send(command);
		log('Redshift credentials are valid');
		return true;
	} catch (error) {
		log(`Error verifying Redshift credentials:\n${error.message}`, error);
		return error.message;
	}
}

module.exports = loadToRedshift;
