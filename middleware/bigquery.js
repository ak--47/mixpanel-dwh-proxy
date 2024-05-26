/*
----
BIGQUERY MIDDLEWARE
----
*/

const { BigQuery } = require("@google-cloud/bigquery");
/** @typedef { import('../types.js').BigQueryTypes } BQTypes */
/** @typedef {import('@google-cloud/bigquery').BigQuery} BQClient */


const NODE_ENV = process.env.NODE_ENV || "prod";
const u = require("ak-tools");
const schemas = require("./bigquery-schemas.js");
const log = require("../components/logger.js");

// CORE MIDDLEWARE CONTRACT
/** @typedef {import('../types').Entities} Entities */
/** @typedef {import('../types').Endpoints} Endpoints */
/** @typedef {import('../types').TableNames} TableNames */
/** @typedef {import('../types').Schema} Schema */
/** @typedef {import('../types').InsertResult} InsertResult */
// MIXPANEL DATA
/** @typedef {import('../types').FlatEvent} Event */
/** @typedef {import('../types').FlatUserUpdate} UserUpdate */
/** @typedef {import('../types').FlatGroupUpdate} GroupUpdate */
/** @typedef {Event[] | UserUpdate[] | GroupUpdate[]} DATA */


//these vars should be cached and only run once when the server starts
/** @type {BQClient} */
let client;
let bigquery_dataset;
let bigquery_project;
let bigquery_keyfile;
let bigquery_service_account;
let bigquery_service_account_pass;
let isClientReady;
let isDatasetReady;
let areTablesReady;




/**
 * Main function to handle BigQuery data insertion
 * this function is called in the main server.js file 
 * and will be called repeatedly as clients stream data in (from client-side SDKs)
 * @param  {DATA} data
 * @param  {Endpoints} type
 * @param  {TableNames} tableNames
 * @return {Promise<InsertResult>}
 *
 */
async function main(data, type, tableNames) {
	const startTime = Date.now();
	const init = await initializeBigQuery(tableNames);
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

	const table = client.dataset(bigquery_dataset).table(targetTable);

	// @ts-ignore
	const result = await insertData(data, table, getBigQuerySchema(type));
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;

}

async function initializeBigQuery(tableNames) {
	// ENV STUFF
	({ bigquery_dataset = "", bigquery_project, bigquery_keyfile, bigquery_service_account, bigquery_service_account_pass } =
		process.env);
	const { eventTable, userTable, groupTable } = tableNames;
	if (!isClientReady) {
		isClientReady = await verifyBigQueryCredentials();
		if (!isClientReady) throw new Error("BigQuery credentials verification failed.");
	}

	if (!isDatasetReady) {
		isDatasetReady = await verifyOrCreateDataset();
		if (!isDatasetReady) throw new Error("Dataset verification or creation failed.");
	}

	if (!areTablesReady) {
		const tableCheckResults = await verifyOrCreateTables([["track", eventTable], ["user", userTable], ["group", groupTable]]);
		areTablesReady = tableCheckResults.every(result => result);
		if (!areTablesReady) throw new Error("Table verification or creation failed.");
	}

	return [isClientReady, isDatasetReady, areTablesReady];
}

async function verifyBigQueryCredentials() {

	// credentials or keyfile or application default credentials
	/** @type {import('@google-cloud/bigquery').BigQueryOptions} */
	const auth = {};
	if (bigquery_keyfile) {
		auth.keyFilename = bigquery_keyfile;
		auth.keyFile = bigquery_keyfile;
	}
	if (bigquery_project) auth.projectId = bigquery_project;
	if (bigquery_service_account && bigquery_service_account_pass) {
		auth.credentials = {
			client_email: bigquery_service_account,
			private_key: bigquery_service_account_pass,
		};
	}

	client = new BigQuery(auth);

	const query = "SELECT 1";
	try {
		const [rows] = await client.query(query);
		log("BigQuery credentials are valid");
		return true;
	} catch (error) {
		log("Error verifying BigQuery credentials:", error);
		return error.message;
	}
}

async function verifyOrCreateDataset() {
	const [datasets] = await client.getDatasets();
	const datasetExists = datasets.some((dataset) => dataset.id === bigquery_dataset);
	if (!datasetExists) {
		log(`Dataset ${bigquery_dataset} does not exist. creating...`);
		const [success] = await client.createDataset(bigquery_dataset);
		const [moreDatasets] = await client.getDatasets();
		const moreDatasetExists = moreDatasets.some((dataset) => dataset.id === bigquery_dataset);
		if (!moreDatasetExists) {
			log(`Failed to create dataset ${bigquery_dataset}`);
			return false;
		}
		log(`Dataset ${bigquery_dataset} created.`);
		return true;


	}
	return true;
}
/**
 * @param  {['track' | 'user' | 'group', string][]} tableNames
 */
async function verifyOrCreateTables(tableNames) {
	const results = [];
	const [tables] = await client.dataset(bigquery_dataset).getTables();

	for (const [type, table] of tableNames) {
		const tableExists = tables.some((t) => t.id === table);
		if (!tableExists) {
			log(`Table ${table} does not exist. creating...`);
			// @ts-ignore
			const tableSchema = getBigQuerySchema(type);
			const [newTable] = await client.dataset(bigquery_dataset).createTable(table, { schema: tableSchema });
			const [moreTables] = await client.dataset(bigquery_dataset).getTables();
			const moreTableExists = moreTables.some((t) => t.id === table);
			if (!moreTableExists) {
				log(`Failed to create table ${table}`);
				results.push(false);
			} else {
				log(`Table ${table} created.`);
				const isTableReady = await waitForTableToBeReady(newTable);
				if (isTableReady) results.push(true);
				else results.push(false);
			}
		} else {
			log(`Table ${table} already exists.`);
			const isTableReady = await waitForTableToBeReady(client.dataset(bigquery_dataset).table(table));
			if (isTableReady) results.push(true);
			else results.push(false);
		}

	}
	return results;
}

async function waitForTableToBeReady(table, retries = 20, maxInsertAttempts = 20) {
	log("Checking if table exits...");

	tableExists: for (let i = 0; i < retries; i++) {
		const [exists] = await table.exists();
		if (exists) {
			log(`\tTable is confirmed to exist on attempt ${i + 1}.`);
			break tableExists;
		}
		const sleepTime = u.rand(1000, 5000);
		log(`sleeping for ${u.prettyTime(sleepTime)}; waiting for table exist; attempt ${i + 1}`);
		await u.sleep(sleepTime);

		if (i === retries - 1) {
			log(`Table does not exist after ${retries} attempts.`);
			return false;
		}
	}

	log("\nChecking if table is ready for operations...");
	let insertAttempt = 0;
	while (insertAttempt < maxInsertAttempts) {
		try {
			// Attempt a dummy insert that SHOULD fail, but not because 404
			const dummyRecord = { [u.uid()]: u.uid() };
			await table.insert([dummyRecord]);
			log("...should never get here...");
			return true; // If successful, return true immediately
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				log(
					`\tTable not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1
					}`
				);
				await u.sleep(sleepTime);
				insertAttempt++;
			} else if (error.name === "PartialFailureError") {
				log("\tTable is ready for operations\n");
				return true;
			} else {
				log("should never get here either");
				debugger;
			}
		}
	}
	return false; // Return false if all attempts fail
}

/**
 * insert data into BigQuery
 * @param  {DATA} batch
 * @param  {import('@google-cloud/bigquery').Table} table
 * @param  {Schema} schema
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, table, schema) {
	log("Starting data insertion...\n");
	let result = { status: "born", destination: "bigQuery" };

	/** @type {import('@google-cloud/bigquery').InsertRowsOptions} */
	const options = {
		skipInvalidRows: false,
		ignoreUnknownValues: false,
		raw: false,
		partialRetries: 3,
		schema: schema,
	};

	try {
		const rows = prepareRowsForInsertion(batch, schema);
		const [response] = await table.insert(rows, options);
		result = { status: "success", insertedRows: rows.length, failedRows: 0, destination: "bigQuery" };
	} catch (error) {
		debugger;
		if (error.name === "PartialFailureError") {
			const failedRows = error.errors.length;
			const insertedRows = batch.length - failedRows;
			const uniqueErrors = Array.from(new Set(error.errors.map((e) => e.errors.map((e) => e.message)).flat()));
			result = {
				status: "error",
				type: "partial failure",
				failedRows,
				insertedRows,
				errors: uniqueErrors,
				destination: "bigQuery",

			};
			log(`Partial failure`);
		}

		else {
			throw error;
		}



	}

	log("\n\tData insertion complete.\n");
	return { ...result, dest: "bigQuery" };
}

/**
 * @param  {Entities & Endpoints} type
 */
function getBigQuerySchema(type) {
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
 * @param  {any} value
 * @param  {BQTypes} type
 */
function convertField(value, type) {
	switch (type) {
		case "STRING":
			return value?.toString();
		case "TIMESTAMP":
			return value;
		case "DATE":
			return value;
		case "INT64":
			return parseInt(value);
		case "FLOAT64":
			return parseFloat(value);
		case "BOOLEAN":
			if (typeof value === "boolean") return value;
			if (typeof value === "number") return value === 1;
			if (typeof value === "string") return value?.toLowerCase() === "true";
		case "RECORD":
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === "object") return JSON.stringify(value);
			if (typeof value === "string") return value;
		case "JSON":
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === "object") return JSON.stringify(value);
			if (typeof value === "string") return value;
		case "STRUCT":
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === "object") return JSON.stringify(value);
			if (typeof value === "string") return value;
		default:
			return value;
	}
}

function prepareRowsForInsertion(batch, schema) {
	const currentTime = new Date().toISOString();
	return batch.map((row) => {
		const newRow = {};
		newRow.insert_time = currentTime;
		// schematized fields
		const schematizedFields = Array.from(
			new Set(
				Object.entries(schema)
					.flat()
					.filter(a => typeof a !== 'string')
					.flat()
					.filter(a => a.name !== "properties").map(a => a.name)
			)
		);
		schematizedFields.forEach((field) => {
			if (row[field] !== undefined) {
				try {
					newRow[field] = convertField(row[field], 'STRING');
					delete row[field];
				} catch (error) {
					debugger;
				}
			}
		});
		newRow.properties = convertField({ ...row }, "JSON");
		return newRow;
	});
}

/**
 * drops all 3 mixpanel tables... this is a destructive operation
 * @param  {TableNames} tableNames
 */
async function dropTables(tableNames) {
	const [allTables] = await client.dataset(bigquery_dataset).getTables();
	const targetTables = Object.values(tableNames);
	// @ts-ignore
	const tablesToDrop = allTables.filter((table) => targetTables.includes(table.id));
	const dropPromises = tablesToDrop.map(async (table) => {
		await table.delete();
	});
	const result = await Promise.all(dropPromises);
	return result;
}

main.drop = dropTables;
main.init = initializeBigQuery;
module.exports = main;