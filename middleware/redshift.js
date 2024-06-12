/*
----
REDSHIFT MIDDLEWARE
----
*/
const { RedshiftDataClient, ExecuteStatementCommand, DescribeStatementCommand } = require('@aws-sdk/client-redshift-data');
const log = require('../components/logger.js');
const u = require('ak-tools');
const { schematizeForWarehouse } = require('../components/transforms.js');
const schemas = require('./redshift-schemas.js');
const { insertWithRetry } = require("../components/retries.js");

const NODE_ENV = process.env.NODE_ENV || "prod";
let MAX_RETRIES = process.env.MAX_RETRIES || 5;
if (typeof MAX_RETRIES === "string") MAX_RETRIES = parseInt(MAX_RETRIES);
if (NODE_ENV === 'test') {
	log.verbose(true);
	log.cli(true);
}

// CORE MIDDLEWARE CONTRACT
/** @typedef {import('../types').Entities} Entities */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TableNames */
/** @typedef {import('../types').Schema} Schema */
/** @typedef {import('../types').InsertResult} InsertResult */
/** @typedef {import('../types').SchematizedData} WarehouseData */
/** @typedef {import('../types').FlatData} FlatData */

// Cached variables to run once when the server starts
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
let isSchemaReady;
// let isDatasetReady;
let areTablesReady;

/**
 * Main function to handle Redshift data insertion
 * this function is called in the main server.js file 
 * and will be called repeatedly as clients stream data in (from client-side SDKs)
 * @param  {FlatData} data
 * @param  {Endpoints} type
 * @param  {TableNames} tableNames
 * @return {Promise<InsertResult>}
 */
async function main(data, type, tableNames) {
	const startTime = Date.now();
	const init = await initializeRedshift(tableNames);
	if (!init.every(i => i)) throw new Error("Failed to initialize Redshift middleware.");
	const { eventTable, userTable, groupTable } = tableNames;

	// Determine the target table based on the record type
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
	const result = await insertWithRetry(insertData, preparedData, targetTable, schema);
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;
}

async function initializeRedshift(tableNames) {
	// Environment variables
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

	// HACK! todo: fix this
	redshift_schema_name = "public";

	const { eventTable, userTable, groupTable } = tableNames;

	if (!isClientReady) {
		isClientReady = await createRedshiftClient();
		if (!isClientReady) throw new Error("Redshift client creation failed.");
	}

	// if (!isDatasetReady) {
	// 	isDatasetReady = await verifyOrCreateDatabase();
	// 	if (!isDatasetReady) throw new Error("Database verification or creation failed.");
	// }

	// if (!isSchemaReady) {
	// 	isSchemaReady = await verifyOrCreateSchema();
	// 	if (!isSchemaReady) throw new Error("Schema verification or creation failed.");
	// }



	if (!areTablesReady) {
		const tableCheckResults = await verifyOrCreateTables([["track", eventTable], ["user", userTable], ["group", groupTable]]);
		areTablesReady = tableCheckResults.every(result => result);
		if (!areTablesReady) throw new Error("Table verification or creation failed.");
	}

	return [isClientReady, areTablesReady];
}

async function createRedshiftClient() {
	const credentials = {
		accessKeyId: redshift_access_key_id,
		secretAccessKey: redshift_secret_access_key,
	};
	if (redshift_session_token) credentials.sessionToken = redshift_session_token;

	const clientConfig = { region: redshift_region, credentials };

	redshiftClient = new RedshiftDataClient(clientConfig);


	try {
		const verifyResult = await executeSQL('SELECT 1', false, "dev");
		if (verifyResult === 1) {
			log('[REDSHIFT] client created and credentials are valid.');
			return true;
		}
		else {
			throw new Error('Failed to verify Redshift credentials');
		}
	} catch (error) {
		return false;
	}
}

async function verifyOrCreateTables(tableNames) {
	const results = [];

	for (const [type, table] of tableNames) {
		const tableExists = await checkIfTableExists(table);
		if (!tableExists) {
			log(`[REDSHIFT] Table ${table} does not exist. Creating...`);
			const tableSchema = getRedshiftSchema(type);
			const sqlSchema = tableSchema.map(f => `${f.name} ${f.type}`).join(", ");
			const tableCreationResult = await createTable(table, sqlSchema);
			// const tableReady = await waitForTableToBeReady(table);
			results.push(true);
			// if (tableReady) {
			// 	log(`[REDSHIFT] Table ${table} created and ready.`);
			// } else {
			// 	log(`[REDSHIFT] Failed to create table ${table}`);
			// }
		} else {
			log(`[REDSHIFT] Table ${table} already exists.`);
			// const tableReady = await waitForTableToBeReady(table);
			results.push(true);
		}
	}

	return results;
}

async function checkIfTableExists(tableName) {
	const checkTableQuery = `SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '${tableName}'`;
	const result = await executeSQL(checkTableQuery);
	return result && result > 0;
}

async function createTable(tableName, schema) {
	const createTableQuery = `CREATE TABLE IF NOT EXISTS ${redshift_schema_name}.${tableName} (${schema})`;
	const createTableResult = await executeSQL(createTableQuery);
	return createTableResult;
}

async function insertData(batch, table, schema) {
	log("[REDSHIFT] Starting data insertion...");
	let result = { status: "born", dest: "redshift" };

	const columnNames = schema.map(f => f.name).join(", ");
	const valuesArray = batch.map(row => {
		const rowValues = schema.map(field => formatSQLValue(row[field.name], field.type)).join(", ");
		return `(${rowValues})`;
	});
	const valuesString = valuesArray.join(", ");
	const insertSQL = `INSERT INTO ${redshift_schema_name}.${table} (${columnNames}) VALUES ${valuesString}`;

	const start = Date.now();
	try {
		const insertResult = await executeSQL(insertSQL, true);
		const duration = Date.now() - start;
		result = { ...result, duration, status: 'success', insertedRows: batch.length, failedRows: 0 };
	} catch (error) {
		const duration = Date.now() - start;
		result = { ...result, status: 'error', errorMessage: error.message, errors: error, duration, insertedRows: 0, failedRows: batch.length };
		log(`[REDSHIFT] Error inserting: ${error.message}`, error, batch);
	}

	log('[REDSHIFT] Data insertion complete;');
	return result;
}


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
async function executeSQL(sql, isBatch = false, altDb = "", altWrkgrp = "") {
	const options = { Sql: sql, Database: redshift_database, WorkgroupName: redshift_workgroup };
	if (altDb) options.Database = altDb;
	if (altWrkgrp) options.WorkgroupName = altWrkgrp;

	const executeCommand = new ExecuteStatementCommand(options);

	try {
		const statement = await redshiftClient.send(executeCommand);
		// if (!isBatch) return null;

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
			if (statementStatus !== 'FINISHED') {
				const waitTime = u.rand(250, 420);
				// log(`[REDSHIFT] Statement ${statementId} is ${statementStatus}. Waiting ${waitTime}ms before checking again...`);
				await u.sleep(waitTime);
			}
		} while (statementStatus !== 'FINISHED');

		const { ResultRows = null } = describeResponse;
		return ResultRows;

	} catch (error) {
		log(`[REDSHIFT] Failed executing SQL:${error.message}`);
		if (NODE_ENV === 'test') debugger;
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
			return value.toString().toLowerCase() === 'true' ? 'TRUE' : 'FALSE';
		case 'STRING':
		case 'VARCHAR':
		case 'DATE':
		case 'TIMESTAMP':
			return `'${value.replace(/'/g, "''")}'`;
		case 'SUPER':
			if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
			if (Array.isArray(value)) return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
			if (typeof value === 'object') return `'${JSON.stringify(value).replace(/'/g, "''")}'`;

		default:
			return value;
	}
}


async function dropTables(tableNames) {
	log(`[REDSHIFT] Dropping tables...`);
	const targetTables = Object.values(tableNames);
	const droppedTables = [];
	const dropPromises = targetTables.map(async (table) => {
		const dropTableQuery = `DROP TABLE IF EXISTS ${redshift_schema_name}.${table}`;
		droppedTables.push(table);
		return await executeSQL(dropTableQuery);
	});
	const results = await Promise.all(dropPromises);
	log(`[REDSHIFT] Dropped tables: ${targetTables.join(', ')}`);
	return { numDropped: droppedTables.length, tablesDropped: droppedTables };

}

main.drop = dropTables;
main.init = initializeRedshift;
module.exports = main;


//todo this doesn't work
async function verifyOrCreateSchema() {
	const checkSchemaQuery = `SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${redshift_schema_name}'`;
	const result = await executeSQL(checkSchemaQuery);
	if (!result || result.length === 0) {
		log(`[REDSHIFT] Schema ${redshift_schema_name} does not exist. Creating...`);
		const createSchemaQuery = `CREATE SCHEMA ${redshift_schema_name}`;
		await executeSQL(createSchemaQuery);
		log(`[REDSHIFT] Schema ${redshift_schema_name} created.`);
	} else {
		log(`[REDSHIFT] Schema ${redshift_schema_name} already exists.`);
	}
	return true;
}


// async function waitForTableToBeReady(tableName, retries = 20, maxInsertAttempts = 20) {
// 	log(`[REDSHIFT] Checking if table ${tableName} exists...`);

// 	for (let i = 0; i < retries; i++) {
// 		const exists = await checkIfTableExists(tableName);
// 		if (exists) {
// 			log(`[REDSHIFT] Table ${tableName} is confirmed to exist on attempt ${i + 1}.`);
// 			break;
// 		}
// 		const sleepTime = Math.random() * (5000 - 1000) + 1000;
// 		log(`[REDSHIFT] Sleeping for ${sleepTime} ms; waiting for table existence; attempt ${i + 1}`);
// 		await new Promise(resolve => setTimeout(resolve, sleepTime));

// 		if (i === retries - 1) {
// 			log(`[REDSHIFT] Table ${tableName} does not exist after ${retries} attempts.`);
// 			return false;
// 		}
// 	}

// 	log(`[REDSHIFT] Checking if table ${tableName} is ready for operations...`);
// 	for (let insertAttempt = 0; insertAttempt < maxInsertAttempts; insertAttempt++) {
// 		try {
// 			const dummyRecord = { "dummy_column": "dummy_value" };
// 			const dummyInsert = await insertDummyRecord(tableName, dummyRecord);
// 			if (dummyInsert) {
// 				log(`[REDSHIFT] Table ${tableName} is ready for operations`);
// 				return true;
// 			}
// 			if (!dummyInsert) {
// 				log(`[REDSHIFT] Table ${tableName} is not ready for operations`);
// 				throw "retry";
// 			}
// 		} catch (error) {
// 			const sleepTime = Math.random() * (5000 - 1000) + 1000;
// 			log(`[REDSHIFT] Sleeping ${sleepTime} ms for table ${tableName}, retrying... attempt #${insertAttempt + 1}`);
// 			await new Promise(resolve => setTimeout(resolve, sleepTime));
// 		}
// 	}
// 	return false;
// }

// async function insertDummyRecord(tableName, dummyRecord) {
// 	const columns = Object.keys(dummyRecord).join(", ");
// 	const values = Object.values(dummyRecord).map(value => `'${value}'`).join(", ");
// 	const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
// 	const result = await executeSQL(insertQuery, true);

// 	if (result instanceof Error && result.message.includes('lock')) {
// 		return false;
// 	}
// 	return true;
// }