/*
----
SNOWFLAKE MIDDLEWARE
----
*/
const snowflake = require('snowflake-sdk');
const { createSnowpipeAPI } = require('snowflake-ingest-node');
const { schematizeForWarehouse } = require('../components/parser.js');
const path = require('path');
const { writeFile, unlink } = require('fs/promises');
const { uid } = require('ak-tools');
const { tmpdir } = require('os');
const dayjs = require('dayjs');


/** @typedef { import('../types.js').SnowflakeTypes } SnowflakeTypes */
/** @typedef {import('snowflake-sdk').Connection} SnowflakeConnection */

const NODE_ENV = process.env.NODE_ENV || "prod";
const TEMP_DIR = NODE_ENV === 'prod' ? path.resolve(tmpdir()) : path.resolve('./tmp');
const TODAY = dayjs().format('YYYY-MM-DD');
let MAX_RETRIES = process.env.MAX_RETRIES || 5;
if (typeof MAX_RETRIES === "string") MAX_RETRIES = parseInt(MAX_RETRIES);
const u = require("ak-tools");
const schemas = require("./snowflake-schemas.js");
const log = require("../components/logger.js");



// CORE MIDDLEWARE CONTRACT
/** @typedef {import('../types').Entities} Entities */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TableNames */
/** @typedef {import('../types').Schema} Schema */
/** @typedef {import('../types').InsertResult} InsertResult */
/** @typedef {import('../types').SchematizedData} WarehouseData */
/** @typedef {import('../types').FlatData} FlatData */


//these vars should be cached and only run once when the server starts
/** @type {SnowflakeConnection} */
let connection;
let snowflake_account;
let snowflake_user;
let snowflake_password;
let snowflake_database;
let snowflake_schema;
let snowflake_warehouse;
let snowflake_role;
let snowflake_access_url;

let isConnectionReady;
let isDatasetReady;
let areTablesReady;

//required for copy into
let snowflake_stage;
let isStageReady;

// required for pipelines
let snowflake_pipe;
let isPipeReady;
let isSnowPipeReady;
/** @type {import('snowflake-ingest-node').SnowpipeAPI} */
let snowpipeAPI;
let snowflake_private_key;
let snowflake_region;
let snowflake_provider;

let transport = 'none';


/**
 * Main function to handle Snowflake data insertion
 * this function is called in the main server.js file 
 * and will be called repeatedly as clients stream data in (from client-side SDKs)
 * @param  {FlatData} data
 * @param  {Endpoints} type
 * @param  {TableNames} tableNames
 * @return {Promise<InsertResult>}
 *
 */
async function main(data, type, tableNames) {
	const startTime = Date.now();
	const init = await initializeSnowflake(tableNames);
	const { eventTable, userTable, groupTable } = tableNames;
	// now we know the tables is ready and we can insert data; this runs repeatedly
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
	const schema = getSnowflakeSchema(type);
	const preparedData = schematizeForWarehouse(data, schema);
	let result;

	switch (transport) {
		case "insert":
			result = await insertWithRetry(preparedData, targetTable, schema);
			break;
		case "copy":
			result = await copyIntoData(preparedData, targetTable, schema);
			break;
		case "pipe":
			result = await insertWithPipe(preparedData, targetTable, schema);
			break;
		default:
			throw new Error("Invalid transport method");
	}

	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;
}

async function initializeSnowflake(tableNames) {
	// ENV STUFF
	({
		snowflake_account,
		snowflake_user,
		snowflake_password,
		snowflake_database,
		snowflake_schema,
		snowflake_warehouse,
		snowflake_role,
		snowflake_access_url,
		snowflake_stage,
		snowflake_pipe,
		snowflake_private_key,
		snowflake_region,
		snowflake_provider,
		// @ts-ignore
		MAX_RETRIES
	} = process.env);

	const { eventTable, userTable, groupTable } = tableNames;
	if (!isConnectionReady) {
		isConnectionReady = await createSnowflakeConnection();
		if (!isConnectionReady) throw new Error("snowflake credentials verification failed.");
		isConnectionReady = await connection.isValidAsync();
		if (!isConnectionReady) throw new Error("snowflake connection is in an invalid state.");
		log("Snowflake connection is ready.");
	}

	if (!isDatasetReady) {
		isDatasetReady = await verifyOrCreateDatabase();
		if (!isDatasetReady) throw new Error("Dataset verification or creation failed.");
		log("Snowflake dataset is ready.");
	}

	if (!areTablesReady) {
		const tableCheckResults = await verifyOrCreateTables([["track", eventTable], ["user", userTable], ["group", groupTable]]);
		areTablesReady = tableCheckResults.every(result => result);
		if (!areTablesReady) throw new Error("Table verification or creation failed.");
		transport = "insert";
		log("using INSERT + BIND transport method.");
	}

	const result = [isConnectionReady, isDatasetReady, areTablesReady];

	//stage uses copy into
	if (snowflake_stage) {
		if (!isStageReady) {
			const stageCheckResults = await verifyOrCreateStage();
			isStageReady = stageCheckResults;
			result.push(isStageReady);
			transport = "copy";
			log("using COPY INTO transport method.");
		}
	}

	//pipe uses a streaming something...
	if (snowflake_pipe) {
		if (!isPipeReady) {
			const pipeCheckResults = await verifyOrCreatePipe(tableNames);
			isPipeReady = pipeCheckResults;
			result.push(isPipeReady);
			transport = "pipe";
			log("using SNOWPIPE transport method.");
		}

		if (!isSnowPipeReady) {
			isSnowPipeReady = await createSnowpipeConnection();
			result.push(isSnowPipeReady);

		}
	}

	// transport = "copy"
	return result;
}


//todo auth is bad here... needs priv key
async function createSnowpipeConnection() {
	try {
		log("Creating Snowpipe connection...");
		const args = [snowflake_user, snowflake_private_key, snowflake_account];
		if (snowflake_region) args.push(snowflake_region);
		if (snowflake_provider) args.push(snowflake_provider);
		// @ts-ignore
		snowpipeAPI = await createSnowpipeAPI(...args);
		log("Snowpipe connection created.");
		return true;
	}
	catch (error) {
		log("Failed to create Snowpipe connection:", error);
		dropTables;
		return false;
	}
}

// main snowflake connection
async function createSnowflakeConnection() {
	if (!snowflake_account) throw new Error('snowflake_account is required');
	if (!snowflake_user) throw new Error('snowflake_user is required');
	if (!snowflake_password) throw new Error('snowflake_password is required');
	if (!snowflake_database) throw new Error('snowflake_database is required');
	if (!snowflake_schema) throw new Error('snowflake_schema is required');
	if (!snowflake_warehouse) throw new Error('snowflake_warehouse is required');
	if (!snowflake_role) throw new Error('snowflake_role is required');
	snowflake.configure({ keepAlive: true, logLevel: 'WARN' });
	log('Attempting to connect to Snowflake...');

	return new Promise((resolve, reject) => {
		const attemptConnect = snowflake.createConnection({
			account: snowflake_account,
			username: snowflake_user,
			password: snowflake_password,
			database: snowflake_database,
			schema: snowflake_schema,
			warehouse: snowflake_warehouse,
			role: snowflake_role,
			accessUrl: snowflake_access_url,
		});

		attemptConnect.connect((err, conn) => {
			if (err) {
				log('Failed to connect to Snowflake:', err);
				debugger;
				resolve(false);
			} else {
				log('Successfully connected to Snowflake');
				connection = conn;
				resolve(true);
			}
		});
	});
}


async function verifyOrCreateDatabase(databaseName = snowflake_database, schemaName = snowflake_schema) {
	const checkDatabaseQuery = `SELECT COUNT(*) AS count FROM ${databaseName.toUpperCase()}.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '${databaseName.toUpperCase()}'`;
	const checkResult = await executeSQL(checkDatabaseQuery, undefined, true);

	let databaseExists = false;
	// @ts-ignore
	if (checkResult?.message?.includes(`Database '${snowflake_database}' does not exist or not authorized.`)) databaseExists = false;
	else if (checkResult?.[0]?.COUNT > 0) databaseExists = true;
	else debugger;


	if (!databaseExists) {
		log(`Database ${databaseName} does not exist. Creating...`);

		const createDatabaseQuery = `CREATE OR REPLACE DATABASE ${databaseName}`;
		const databaseCreationResult = await executeSQL(createDatabaseQuery);

		log(`Database ${databaseName} created.`);

	} else {
		log(`Database ${databaseName} already exists.`);
	}

	// Check if the schema exists
	const checkSchemaQuery = `SHOW SCHEMAS IN DATABASE ${databaseName}`;
	const checkSchemaResult = await executeSQL(checkSchemaQuery, undefined, true);
	if (!Array.isArray(checkSchemaResult)) throw new Error("Failed to check schema existence");
	const schemaExists = checkSchemaResult?.some(schema => schema.name === schemaName.toUpperCase());

	if (!schemaExists) {
		log(`Schema ${schemaName} does not exist in database ${databaseName}. Creating...`);

		const createSchemaQuery = `CREATE SCHEMA ${databaseName}.${schemaName}`;
		const schemaCreateResult = await executeSQL(createSchemaQuery);

		log(`Schema ${schemaName} created in database ${databaseName}.`);
	} else {
		log(`Schema ${schemaName} already exists in database ${databaseName}.`);
	}

	// Set the current schema
	const useSchemaQuery = `USE SCHEMA ${databaseName}.${schemaName}`;
	const useSchemaResult = await executeSQL(useSchemaQuery);

	log(`Using schema ${schemaName} in database ${databaseName}.`);

	return true;
}

async function verifyOrCreateTables(tableNames) {
	const results = [];

	for (const [type, table] of tableNames) {
		const tableExists = await checkIfTableExists(table);
		if (!tableExists) {
			log(`Table ${table} does not exist. Creating...`);
			const tableSchema = getSnowflakeSchema(type);
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

async function verifyOrCreateStage() {
	const checkStageQuery = `SHOW STAGES LIKE '${snowflake_stage}'`;
	const result = await executeSQL(checkStageQuery);
	if (!Array.isArray(result)) throw new Error("Failed to check stage existence");
	if (!result || result.length === 0) {
		log(`Stage ${snowflake_stage} does not exist. Creating...`);
		const createStageQuery = `CREATE OR REPLACE STAGE ${snowflake_stage} FILE_FORMAT = (TYPE = 'JSON') DIRECTORY = (ENABLE = TRUE)`;
		const stageCreatedResult = await executeSQL(createStageQuery);
		log(`Stage ${snowflake_stage} created.`);

		// Grant necessary permissions
		const grantReadQuery = `GRANT READ ON STAGE ${snowflake_stage} TO ROLE ${snowflake_role}`;
		const grantWriteQuery = `GRANT WRITE ON STAGE ${snowflake_stage} TO ROLE ${snowflake_role}`;
		const readPermsResult = await executeSQL(grantReadQuery);
		const writePermsResult = await executeSQL(grantWriteQuery);
		log(`Granted READ and WRITE on stage ${snowflake_stage} to role ${snowflake_role}`);
	} else {
		log(`Stage ${snowflake_stage} already exists.`);
	}
	return true;
}

async function verifyOrCreatePipe(tableNames) {
	const { eventTable, userTable, groupTable } = tableNames;
	const allTables = Object.entries(tableNames); // Ensure to verifyOrCreatePipe for all 3 tables

	for (const [type, table] of allTables) {
		const checkPipeQuery = `SHOW PIPES LIKE '${snowflake_pipe}_${table}'`;
		const result = await executeSQL(checkPipeQuery);
		if (!Array.isArray(result) || result.length === 0) {
			log(`Pipe ${snowflake_pipe}_${table} does not exist. Creating...`);

			const schema = getSnowflakeSchema(type.split('Table').shift());
			const columnMappings = schema
				.map(col => `$1:${col.name.toLowerCase()} AS ${col.name}`)
				.join(', ');

			const createPipeQuery = `
				CREATE OR REPLACE PIPE ${snowflake_pipe}_${table} AS
				COPY INTO ${table}
				FROM (
					SELECT ${columnMappings}
					FROM @${snowflake_stage}
				)
				FILE_FORMAT = (TYPE = 'JSON')
				ON_ERROR = 'CONTINUE';
			`;
			const createPipeResult = await executeSQL(createPipeQuery);
			log(`Pipe ${snowflake_pipe}_${table} created.`);
		} else {
			log(`Pipe ${snowflake_pipe}_${table} already exists.`);
		}
	}
	return true;
}


/**
 * insert data into snowflake; the most basic way
 * @param  {WarehouseData} batch
 * @param  {string} table
 * @param  {Schema} schema
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, table, schema) {
	log("Append rows via INSERT...\n");
	let result = { status: "born", dest: "snowflake" };
	// Insert data
	const [insertSQL, hasVariant] = prepareInsertSQL(schema, table);
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
		log(`Inserting ${batch.length} rows into ${table}...`);
		const task = await executeSQL(insertSQL, data);
		const duration = Date.now() - start;
		const insertedRows = task?.[0]?.['number of rows inserted'] || 0;
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

/**
 * insert data into snowflake using stages and copy into
 * @param  {WarehouseData} batch
 * @param  {string} table
 * @param  {Schema} schema
 * @return {Promise<InsertResult>}
 */
async function copyIntoData(batch, table, schema) {
	log("Appending data using COPY INTO...\n");
	let result = { status: "born", dest: "snowflake" };

	const FILE_PATH = path.resolve(TEMP_DIR, `${TODAY}_${uid(42)}.json`);
	const fileName = path.basename(FILE_PATH);

	// Prepare data to be uploaded to the stage
	const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');

	// Write data to a temporary file
	const writeFileResult = await writeFile(FILE_PATH, dataToUpload);

	// Use the PUT command to upload the file to the Snowflake stage
	const stageName = `@${snowflake_stage}`;
	const putCommand = `PUT file://${FILE_PATH} ${stageName}`;
	try {
		const putResult = await executeSQL(putCommand);
		log(`File ${FILE_PATH} uploaded to stage ${stageName}`);
	} catch (error) {
		log(`Error uploading file to stage: ${error.message}`, error);
		throw error;
	}

	// Generate the column mappings for the COPY INTO command
	const columnMappings = schema.map(col => `$1:${col.name.toLowerCase()} AS ${col.name}`).join(', ');

	//todo: CONSIDER CALLING COPY INTO ONLY ~5% of the time
	// instead we would call flushStageToTable every 5% of the time and deleteAllFilesFromStage

	// Use the COPY INTO command to load the data from the stage into the table
	const copyCommand = `
	  COPY INTO ${table}
	  FROM (
		SELECT ${columnMappings}
		FROM ${stageName}/${fileName}
	  )
	  FILE_FORMAT = (TYPE = 'JSON');
	`;
	try {
		const copyIntoResult = await executeSQL(copyCommand);
		log(`Data copied from stage ${stageName} into table ${table}`);
		result.status = 'success';
		result.insertedRows = batch.length;
		result.failedRows = 0;

		const removeCommand = `REMOVE ${stageName}/${fileName}`;
		const removeFileResult = await executeSQL(removeCommand);
		log(`File ${fileName} removed from stage ${stageName}`);

	} catch (error) {
		log(`Error copying data into table: ${error.message}`, error);
		throw error;
	} finally {
		// Remove the temporary file
		const removeLocalFileResult = await unlink(FILE_PATH);
	}

	log("Data insertion using Snowpipe complete.\n");
	return result;
}


/**
 * an attempt to use snowpipe to stream data into snowflake
 * ? https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview
 * @param  {WarehouseData} batch
 * @param  {string} table
 * @param  {Schema} schema
 */
async function insertWithPipe(batch, table, schema) {
	log("Appending data using Snowpipe...\n");
	let result = { status: "born", dest: "snowflake" };

	const FILE_PATH = path.resolve(TEMP_DIR, `${TODAY}_${uid(42)}.json`);
	const fileName = path.basename(FILE_PATH);

	// Prepare data to be uploaded to the stage
	const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');

	// Write data to a temporary file
	const localFileResult = await writeFile(FILE_PATH, dataToUpload);

	// Use the PUT command to upload the file to the Snowflake stage
	const stageName = `@${snowflake_stage}`;
	const putCommand = `PUT file://${FILE_PATH} ${stageName}`;
	try {
		const putResult = await executeSQL(putCommand);
		log(`File ${FILE_PATH} uploaded to stage ${stageName}`);
	} catch (error) {
		log(`Error uploading file to stage: ${error.message}`, error);
		throw error;
	}

	// Notify Snowpipe about the new file in the stage
	const pipeName = `${snowflake_database}.${snowflake_schema}.${snowflake_pipe}_${table}`;
	try {
		const response = await snowpipeAPI.insertFile(pipeName, [fileName]);
		log(`File ${fileName} inserted to Snowpipe ${pipeName}`);
		result.status = 'success';
		result.insertedRows = batch.length;
		result.failedRows = 0;

		const report = await snowpipeAPI.insertReport(pipeName);
		const history = await snowpipeAPI.loadHistoryScan(pipeName, dayjs().subtract(1, 'day').toISOString(), dayjs().add(1, 'day').toISOString());
		debugger; //! JEFF PLEASE HELP...I CANT GET THE DAMN FILES IN THE COMPUTER

		// Remove the file from the stage after processing
		const removeCommand = `REMOVE ${stageName}/${fileName}`;
		const removeFileResult = await executeSQL(removeCommand);
		log(`File ${fileName} removed from stage ${stageName}`);
	} catch (error) {
		log(`Error notifying Snowpipe: ${error.message}`, error);
		throw error;
	} finally {
		// Remove the temporary local file
		const removeLocalFile = await unlink(FILE_PATH);
	}

	log("Data insertion using Snowpipe complete.\n");
	return result;
}



/**
 * @param  {WarehouseData} batch
 * @param  {string} table
 * @param  {Schema} schema
 */
async function insertWithRetry(batch, table, schema) {
	let attempt = 0;
	const backoff = (attempt) => Math.min(1000 * 2 ** attempt, 30000); // Exponential backoff

	// @ts-ignore
	while (attempt < MAX_RETRIES) {
		try {
			const result = await insertData(batch, table, schema);
			return result;
		} catch (error) {
			if (error.message === 'TableLockedError') {
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

/**
 * Flush all files in a stage to a specified table
 * @param {string} table - The name of the table to copy data into
 * @param {Schema} schema - The schema of the table
 * @param {string} stageName - The name of the stage
 * @return {Promise<InsertResult>}
 */
async function flushStageToTable(table, schema, stageName) {
	log("Flushing all files in stage to table...");

	const columnMappings = schema.map(col => `$1:${col.name.toLowerCase()} AS ${col.name}`).join(', ');

	// Generate the COPY INTO command
	const copyCommand = `
        COPY INTO ${table}
        FROM (
            SELECT ${columnMappings}
            FROM ${stageName}
        )
        FILE_FORMAT = (TYPE = 'JSON')
    `;

	try {
		const copyResult = await executeSQL(copyCommand);
		log(`All files in stage ${stageName} have been copied to table ${table}`);
		return { status: 'success', dest: "Snowflake", message: `All files in stage ${stageName} have been copied to table ${table}` };
	} catch (error) {
		log(`Error copying data from stage to table: ${error.message}`, error);
		throw error;
	}
}

/**
 * Delete all files from a specified stage
 * @param {string} stageName - The name of the stage
 * @return {Promise<InsertResult>}
 */
async function deleteAllFilesFromStage(stageName) {
	log("Deleting all files from stage...");

	// Generate the REMOVE command
	const removeCommand = `REMOVE ${stageName}/*`;

	try {
		const removeStageFilesResult = await executeSQL(removeCommand);
		log(`All files have been removed from stage ${stageName}`);
		return { status: 'success', message: `All files have been removed from stage ${stageName}`, dest: "Snowflake" };
	} catch (error) {
		log(`Error removing files from stage: ${error.message}`, error);
		throw error;
	}
}

async function checkIfTableExists(tableName) {
	const checkTableQuery = `SHOW TABLES LIKE '${tableName}'`;
	const result = await executeSQL(checkTableQuery);
	if (!result) return false;
	if (Array.isArray(result)) {
		if (result.length === 0) return false;
		if (result.length > 0) return true;
	}
	debugger;
	return false;

}

async function createTable(tableName, schema) {
	const createTableQuery = `CREATE OR REPLACE TABLE ${tableName} (${schema})`;
	const createTableResult = await executeSQL(createTableQuery);
	return createTableResult;
}

async function insertDummyRecord(tableName, dummyRecord) {
	const columns = Object.keys(dummyRecord).join(", ");
	const values = Object.values(dummyRecord).map(value => `'${value}'`).join(", ");
	const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
	const result = await executeSQL(insertQuery, undefined, true);
	// @ts-ignore
	const { code, data } = result;
	const { errorCode, sqlState, type } = data;
	if (code !== "000904") return false;
	if (errorCode !== "000904") return false;
	if (sqlState !== "42000") return false;
	if (type !== "COMPILATION") return false;
	return true;

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
function getSnowflakeSchema(type) {
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
 * Executes a given SQL query on the Snowflake connection
 * optional binds for bulk insert
 * @param {string} sql 
 * @param {snowflake.Binds} [binds] pass binds to bulk insert
 * @param {boolean} [neverThrow] whether to throw an error if the query fails
 * @returns {Promise<snowflake.StatementStatus | any[] | undefined | snowflake.SnowflakeError>}
 */
function executeSQL(sql, binds, neverThrow = false) {
	return new Promise((resolve, reject) => {
		const options = { sqlText: sql };
		if (binds) options.binds = binds;
		if (binds) options.parameters = { MULTI_STATEMENT_COUNT: 1 };
		connection.execute({
			...options,
			complete: (err, stmt, rows) => {
				if (err) {
					if (neverThrow) {
						resolve(err);
						return;
					}

					const { code, data, message, name, sqlState, isFatal } = err;
					if (code?.toString() === "000625" && name === "OperationFailedError" && message.includes('has locked table')) {
						reject(new Error('TableLockedError')); // Signals a retry
						return;
					}
					debugger;
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
 * Drops the specified tables in Snowflake. This is a destructive operation.
 * @param {TableNames} tableNames
 */
async function dropTables(tableNames) {
	const results = [];
	const dropTablePromises = Object.values(tableNames).map(async (table) => {
		const dropTableQuery = `DROP TABLE IF EXISTS ${table}`;
		const dropTableResult = await executeSQL(dropTableQuery);
		results.push(dropTableResult);
		const dropPipeQuery = `DROP PIPE IF EXISTS ${snowflake_pipe}_${table}`;
		const dropPipeResult = await executeSQL(dropPipeQuery);
		results.push(dropPipeResult);
	});

	await Promise.all(dropTablePromises);

	// Drop stage and pipe
	const dropStageQuery = `DROP STAGE IF EXISTS ${snowflake_stage}`;
	const dropStageResult = await executeSQL(dropStageQuery);
	results.push(dropStageResult);

	return results.flat();
}





main.drop = dropTables;
main.init = initializeSnowflake;
module.exports = main;
