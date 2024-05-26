/*
----
REDSHIFT MIDDLEWARE
----
*/
const { RedshiftDataClient, ExecuteStatementCommand, DescribeStatementCommand } = require('@aws-sdk/client-redshift-data');
const log = require('../components/logger.js');
const u = require('ak-tools');
const { schematizeForWarehouse } = require('../components/parser.js');
const schemas = require('./redshift-schemas.js');

const NODE_ENV = process.env.NODE_ENV || "prod";
let MAX_RETRIES = process.env.MAX_RETRIES || 5;
if (typeof MAX_RETRIES === "string") MAX_RETRIES = parseInt(MAX_RETRIES);

// CORE MIDDLEWARE CONTRACT
/** @typedef {import('../types').Entities} Entities */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TableNames */
/** @typedef {import('../types').Schema} Schema */
/** @typedef {import('../types').InsertResult} InsertResult */
/** @typedef {import('../types').SchematizedData} WarehouseData */
/** @typedef {import('../types').FlatData} FlatData */

//these vars should be cached and only run once when the server starts
/** @type {RedshiftDataClient} */
let redshiftClient;
let redshift_workgroup;
let redshift_database;
let redshift_region;
let redshift_access_key_id;
let redshift_secret_access_key;
let redshift_schema_name;
let redshift_session_token;
let isClientReady;
let isDatasetReady;
let areTablesReady;

/**
 * Main function to handle Redshift data insertion
 * this function is called in the main server.js file 
 * and will be called repeatedly as clients stream data in (from client-side SDKs)
 * @param  {WarehouseData} data
 * @param  {Endpoints} type
 * @param  {TableNames} tableNames
 * @return {Promise<InsertResult>}
 */
async function main(data, type, tableNames) {
	const startTime = Date.now();
	const init = await initializeRedshift(tableNames);
	const { eventTable, userTable, groupTable } = tableNames;

	// now we know the tables are ready and we can insert data; this runs repeatedly
	let targetTable;
	switch (type) {
		case "track":
			targetTable = eventTable;
			break;
		case "engage":
			targetTable = userTable;
			break;
		case "groups":
			targetTable = groupTable;
			break;
		default:
			throw new Error("Invalid Record Type");
	}

	const schema = getRedshiftSchema(type);
	const preparedData = schematizeForWarehouse(data, schema);
	const result = await insertWithRetry(preparedData, targetTable, schema);
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;
}

async function initializeRedshift(tableNames) {
	// ENV STUFF
	({
		redshift_workgroup,
		redshift_database,
		redshift_region,
		redshift_access_key_id,
		redshift_secret_access_key,
		redshift_schema_name,
		redshift_session_token,
		// @ts-ignore
		MAX_RETRIES
	} = process.env);

	const { eventTable, userTable, groupTable } = tableNames;
	if (!isClientReady) {
		isClientReady = await createRedshiftClient();
		if (!isClientReady) throw new Error("Redshift client creation failed.");
	}

	if (!isDatasetReady) {
		isDatasetReady = await verifyOrCreateDatabase();
		if (!isDatasetReady) throw new Error("Database verification or creation failed.");
	}

	if (!areTablesReady) {
		const tableCheckResults = await verifyOrCreateTables([["track", eventTable], ["user", userTable], ["group", groupTable]]);
		areTablesReady = tableCheckResults.every(result => result);
		if (!areTablesReady) throw new Error("Table verification or creation failed.");
	}

	return [isClientReady, isDatasetReady, areTablesReady];
}

async function createRedshiftClient() {
	const credentials = {
		accessKeyId: redshift_access_key_id,
		secretAccessKey: redshift_secret_access_key,
	};
	if (redshift_session_token) credentials.sessionToken = redshift_session_token;

	/** @type {import('@aws-sdk/client-redshift-data').RedshiftDataClientConfig} */
	const clientConfig = { region: redshift_region, credentials };

	redshiftClient = new RedshiftDataClient(clientConfig);

	try {
		const authentication = await verifyRedshiftCredentials(redshiftClient);
		log('Redshift client created and credentials are valid.');
		return true;
	} catch (error) {
		log('Failed to verify Redshift credentials:', error);
		return false;
	}
}

async function verifyOrCreateDatabase() {
	const checkDatabaseQuery = `SELECT 1 FROM pg_database WHERE datname = '${redshift_database}'`;
	const result = await executeSQL(checkDatabaseQuery);
	if (!result || result.length === 0) {
		log(`Database ${redshift_database} does not exist. Creating...`);
		const createDatabaseQuery = `CREATE DATABASE ${redshift_database}`;
		await executeSQL(createDatabaseQuery);
		log(`Database ${redshift_database} created.`);
	} else {
		log(`Database ${redshift_database} already exists.`);
	}
	return true;
}

async function verifyOrCreateTables(tableNames) {
	const results = [];

	for (const [type, table] of tableNames) {
		const tableExists = await checkIfTableExists(table);
		if (!tableExists) {
			log(`Table ${table} does not exist. Creating...`);
			const tableSchema = getRedshiftSchema(type);
			const sqlSchema = tableSchema.map(f => `${f.name} ${f.type}`).join(", ");
			const tableCreateResult = await createTable(table, sqlSchema);
			const tableReady = await waitForTableToBeReady(table);
			if (tableReady) {
				results.push(true);
				log(`Table ${table} created and ready.`);
			} else {
				results.push(false);
				log(`Failed to create table ${table}`);
			}
		} else {
			log(`Table ${table} already exists.`);
			const tableReady = await waitForTableToBeReady(table);
			if (tableReady) {
				results.push(true);
			} else {
				results.push(false);
			}
		}
	}

	return results;
}

async function checkIfTableExists(tableName) {
	const checkTableQuery = `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '${redshift_schema_name}' AND tablename = '${tableName}'`;
	const result = await executeSQL(checkTableQuery);
	return result && result.length > 0;
}

async function createTable(tableName, schema) {
	const createTableQuery = `CREATE OR REPLACE TABLE ${redshift_schema_name}.${tableName} (${schema})`;
	const createTableResult = await executeSQL(createTableQuery);
	return createTableResult;
}

/**
 * insert data into Redshift
 * @param  {WarehouseData} batch
 * @param  {string} table
 * @param  {Schema} schema
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, table, schema) {
	log("Starting data insertion...\n");
	let result = { status: "born", dest: "redshift" };

	const columnNames = schema.map(f => f.name).join(", ");
	let valuesArray = [];
	for (const row of batch) {
		let rowValues = schema.map(field => {
			let value = row[field.name];
			return formatSQLValue(value, field.type);
		}).join(", ");
		valuesArray.push(`(${rowValues})`);
	}
	const valuesString = valuesArray.join(", ");
	const insertSQL = `INSERT INTO ${redshift_schema_name}.${table} (${columnNames}) VALUES ${valuesString}`;

	const start = Date.now();
	try {
		log(`Inserting ${batch.length} rows into ${table}...`);
		const task = await executeSQL(insertSQL, true);
		const duration = Date.now() - start;
		const insertedRows = task || 0; // If task is null, assume 0 rows inserted
		const failedRows = batch.length - insertedRows;
		result = { ...result, duration, status: 'success', insertedRows, failedRows };

	} catch (error) {
		const duration = Date.now() - start;
		result = { ...result, status: 'error', errorMessage: error.message, errors: error, duration, insertedRows: 0, failedRows: batch.length };
		log(`Error inserting: ${error.message}`, error, batch);
	}

	log('\n\nData insertion complete;\n\n');
	return result;
}

async function insertWithRetry(batch, table, schema) {
	let attempt = 0;
	const backoff = (attempt) => Math.min(1000 * 2 ** attempt, 30000); // Exponential backoff

	// @ts-ignore
	while (attempt < MAX_RETRIES) {
		try {
			const result = await insertData(batch, table, schema);
			return result;
		} catch (error) {
			if (error.message.includes('lock')) {
				const waitTime = backoff(attempt);
				log(`Retry attempt ${attempt + 1}: waiting for ${waitTime} ms before retrying...`);
				await new Promise(resolve => setTimeout(resolve, waitTime));
				attempt++;
			} else {
				throw error;
			}
		}
	}

	throw new Error(`Failed to insert data after ${MAX_RETRIES} attempts`);
}

async function waitForTableToBeReady(tableName, retries = 20, maxInsertAttempts = 20) {
	log(`Checking if table ${tableName} exists...`);

	for (let i = 0; i < retries; i++) {
		const exists = await checkIfTableExists(tableName);
		if (exists) {
			log(`Table ${tableName} is confirmed to exist on attempt ${i + 1}.`);
			break;
		}
		const sleepTime = Math.random() * (5000 - 1000) + 1000;
		log(`Sleeping for ${sleepTime} ms; waiting for table existence; attempt ${i + 1}`);
		await new Promise(resolve => setTimeout(resolve, sleepTime));

		if (i === retries - 1) {
			log(`Table ${tableName} does not exist after ${retries} attempts.`);
			return false;
		}
	}

	log(`Checking if table ${tableName} is ready for operations...`);
	for (let insertAttempt = 0; insertAttempt < maxInsertAttempts; insertAttempt++) {
		try {
			const dummyRecord = { "dummy_column": "dummy_value" };
			const dummyInsert = await insertDummyRecord(tableName, dummyRecord);
			if (dummyInsert) {
				log(`Table ${tableName} is ready for operations`);
				return true;
			}
			if (!dummyInsert) {
				log(`Table ${tableName} is not ready for operations`);
				throw "retry";
			}

		} catch (error) {
			const sleepTime = Math.random() * (5000 - 1000) + 1000;
			log(`sleeping ${sleepTime} ms for table ${tableName}, retrying... attempt #${insertAttempt + 1}`);
			await new Promise(resolve => setTimeout(resolve, sleepTime));

		}
	}
	return false;
}

// HELPERS
function getRedshiftSchema(type) {
	const schemaMappings = {
		event: schemas.eventsSchema,
		track: schemas.eventsSchema,
		user: schemas.usersSchema,
		engage: schemas.usersSchema,
		group: schemas.groupsSchema,
		groups: schemas.groupsSchema,
	};
	const schema = schemaMappings[type];
	if (!schema) throw new Error("Invalid Record Type");
	return schema;
}

/**
 * Executes a given SQL query on the Redshift connection
 * @param {string} sql 
 * @param {boolean} isBatch
 * @returns {Promise<number | any>}
 */
async function executeSQL(sql, isBatch = false) {
	const executeCommand = new ExecuteStatementCommand({
		Sql: sql,
		Database: redshift_database,
		WorkgroupName: redshift_workgroup,
	});

	try {
		const statement = await redshiftClient.send(executeCommand);
		if (!isBatch) return null;
		// Wait for the statement to complete
		const statementId = statement.Id;
		const describeCommand = new DescribeStatementCommand({ Id: statementId });
		let statementStatus;
		let describeResponse;
		do {
			describeResponse = await redshiftClient.send(describeCommand);
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
 * Verifies Redshift credentials by running a simple query
 * @param  {RedshiftDataClient} redshiftClient
 */
async function verifyRedshiftCredentials(redshiftClient) {
	const sql = 'SELECT 1';
	const command = new ExecuteStatementCommand({
		Sql: sql,
		Database: redshift_database,
		WorkgroupName: redshift_workgroup
	});

	try {
		await redshiftClient.send(command);
		log('Redshift credentials are valid');
		return true;
	} catch (error) {
		debugger;
		log(`Error verifying Redshift credentials:\n${error.message}`, error);
		return error.message;
	}
}


async function insertDummyRecord(tableName, dummyRecord) {
    const columns = Object.keys(dummyRecord).join(", ");
    const values = Object.values(dummyRecord).map(value => `'${value}'`).join(", ");
    const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
    const result = await executeSQL(insertQuery, true);

    // Check if the insertion failed due to a lock or other error
    if (result instanceof Error && result.message.includes('lock')) {
        return false; // Signal that the table is not ready
    }
    
    return true;
}


/**
 * Drops the specified tables in Redshift. This is a destructive operation.
 * @param {TableNames} tableNames
 */
async function dropTables(tableNames) {
	const targetTables = Object.values(tableNames);
	const dropPromises = targetTables.map(async (table) => {
		const dropTableQuery = `DROP TABLE IF EXISTS ${redshift_schema_name}.${table}`;
		const dropTableResult = await executeSQL(dropTableQuery);
		return dropTableResult;
	});
	const results = await Promise.all(dropPromises);
	log(`Dropped tables: ${targetTables.join(', ')}`);
	return results.flat();
}

main.drop = dropTables;
main.init = initializeRedshift;
module.exports = main;

