/*
----
BIGQUERY MIDDLEWARE
----
*/

//todo: pubsub fallback

const { BigQuery } = require("@google-cloud/bigquery");
/** @typedef { import('../types.js').BigQueryTypes } BQTypes */
/** @typedef {import('@google-cloud/bigquery').BigQuery} BQClient */

const pubsub = require("./pubsub.js");

const NODE_ENV = process.env.NODE_ENV || "prod";
const u = require("ak-tools");
const { schematizeForWarehouse } = require('../components/transforms.js');
const { insertWithRetry } = require("../components/retries.js");
const schemas = require("./bigquery-schemas.js");
const log = require("../components/logger.js");
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


//these vars should be cached and only run once when the server starts
/** @type {BQClient} */
let client;
let bigquery_dataset;
let bigquery_project;
let bigquery_keyfile;
let bigquery_service_account_email;
let bigquery_service_account_private_key;
let isClientReady;
let isDatasetReady;
let areTablesReady;
let isPubSubReady;
let pubsub_bad_topic;




/**
 * Main function to handle BigQuery data insertion
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
	const init = await initializeBigQuery(tableNames);
	if (!init.every(i => i)) throw new Error("Failed to initialize BigQuery middleware.");
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
	const result = await insertWithRetry(insertData, data, table, getBigQuerySchema(type));
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;

}

async function initializeBigQuery(tableNames) {
	// ENV STUFF
	({
		bigquery_dataset = "",
		bigquery_project,
		bigquery_keyfile,
		bigquery_service_account_email,
		bigquery_service_account_private_key,
		pubsub_bad_topic = ""

	} = process.env);
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

	const ready = [isClientReady, isDatasetReady, areTablesReady];

	// ! PUBSUB FALLBACK INITIALIZATION
	if (pubsub_bad_topic) {
		const pubsubInit = await pubsub.init(pubsub_bad_topic);
		isPubSubReady = true;
		ready.push(isPubSubReady);
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
	if (bigquery_service_account_email && bigquery_service_account_private_key) {
		auth.credentials = {
			client_email: bigquery_service_account_email,
			private_key: bigquery_service_account_private_key,
		};
	}

	client = new BigQuery(auth);

	const query = "SELECT 1";
	try {
		const [rows] = await client.query(query);
		log("[BIGQUERY] credentials are valid");
		return true;
	} catch (error) {
		log("[BIGQUERY] Error verifying BigQuery credentials:", error);
		return error.message;
	}
}

async function verifyOrCreateDataset() {
	const [datasets] = await client.getDatasets();
	const datasetExists = datasets.some((dataset) => dataset.id === bigquery_dataset);
	if (!datasetExists) {
		log(`[BIGQUERY] Dataset ${bigquery_dataset} does not exist. creating...`);
		const [success] = await client.createDataset(bigquery_dataset);
		const [moreDatasets] = await client.getDatasets();
		const moreDatasetExists = moreDatasets.some((dataset) => dataset.id === bigquery_dataset);
		if (!moreDatasetExists) {
			log(`[BIGQUERY] Failed to create dataset ${bigquery_dataset}`);
			return false;
		}
		log(`[BIGQUERY] Dataset ${bigquery_dataset} created.`);
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
			log(`[BIGQUERY] Table ${table} does not exist. creating...`);
			// @ts-ignore
			const tableSchema = getBigQuerySchema(type);

			/** @type {import('@google-cloud/bigquery').TableMetadata} */
			const tableMetaData = {
				schema: tableSchema,
				timePartitioning: {
					type: 'DAY',
					field: type === "track" ? "event_time" : "insert_time"
				},
				clustering: {
					fields: [] // needs to be populated
				}
			};

			if (type === "track") tableMetaData?.clustering?.fields?.push("event");
			if (type === "user") tableMetaData?.clustering?.fields?.push("distinct_id");
			if (type === "group") tableMetaData?.clustering?.fields?.push("group_id");


			const [newTable] = await client.dataset(bigquery_dataset).createTable(table, tableMetaData);
			const [moreTables] = await client.dataset(bigquery_dataset).getTables();
			const moreTableExists = moreTables.some((t) => t.id === table);
			if (!moreTableExists) {
				log(`[BIGQUERY] Failed to create table ${table}`);
				results.push(false);
			} else {
				log(`[BIGQUERY] Table ${table} created.`);
				const isTableReady = await waitForTableToBeReady(newTable);
				if (isTableReady) results.push(true);
				else results.push(false);
			}
		} else {
			log(`[BIGQUERY] Table ${table} already exists.`);
			const isTableReady = await waitForTableToBeReady(client.dataset(bigquery_dataset).table(table));
			if (isTableReady) results.push(true);
			else results.push(false);
		}

	}
	return results;
}

async function waitForTableToBeReady(table, retries = 20, maxInsertAttempts = 20) {
	log("[BIGQUERY] Checking if table exits...");
	const tableName = table.id;
	tableExists: for (let i = 0; i < retries; i++) {
		const [exists] = await table.exists();
		if (exists) {
			log(`[BIGQUERY] Table is confirmed to exist on attempt ${i + 1}.`);
			break tableExists;
		}
		const sleepTime = u.rand(1000, 5000);
		log(`[BIGQUERY] Table sleeping for ${u.prettyTime(sleepTime)}; waiting for table exist; attempt ${i + 1}`);
		await u.sleep(sleepTime);

		if (i === retries - 1) {
			log(`[BIGQUERY] Table does not exist after ${retries} attempts.`);
			return false;
		}
	}

	log("[BIGQUERY] Checking if table is ready for operations...");
	let insertAttempt = 0;
	while (insertAttempt < maxInsertAttempts) {
		try {
			// Attempt a dummy insert that SHOULD fail, but not because 404
			const dummyRecord = { [u.uid()]: u.uid() };
			const dummyInsertResult = await table.insert([dummyRecord]);
			log("[BIGQUERY] ...should never get here...");
			return true; // If successful, return true immediately
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				log(`[BIGQUERY] Table not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1}`);
				await u.sleep(sleepTime);
				insertAttempt++;
			} else if (error.name === "PartialFailureError") {
				log("[BIGQUERY] Table is ready for operations");
				return true;
			} else {
				log("[BIGQUERY] should never get here either");
				if (NODE_ENV === 'test') debugger;
			}
		}
	}
	return false; // Return false if all attempts fail
}

/**
 * insert data into BigQuery
 * @param  {FlatData} batch
 * @param  {import('@google-cloud/bigquery').Table} table
 * @param  {Schema} schema
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, table, schema) {
	log("[BIGQUERY] Starting data insertion...");
	let result = { status: "born" };

	/** @type {import('@google-cloud/bigquery').InsertRowsOptions} */
	const options = {
		skipInvalidRows: false,
		ignoreUnknownValues: false,
		raw: false,
		partialRetries: 3,
		schema: schema,
	};

	try {
		const rows = schematizeForWarehouse(batch, schema);
		// for JSON columns, BQ wants a string
		rows.forEach(row => row.properties = JSON.stringify(row.properties));
		const [response] = await table.insert(rows, options);
		result = { status: "success", insertedRows: rows.length, failedRows: 0 };
	} catch (error) {
		if (NODE_ENV === 'test') debugger;
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

			};
			log(`[BIGQUERY] Partial failure`);
			if (pubsub_bad_topic) {
				const message = {
					table: table.id,
					error: error.name,
					kind: error.response?.kind,
					data: batch,
					details: error.errors,
					schema: schema,
				};
				const pubsubResult = await pubsub.publish(message, pubsub_bad_topic);
				log(`[BIGQUERY] insert error; published to topic ${pubsub_bad_topic}; message id: ${pubsubResult?.messageId}`);
			}
		}

		else {
			if (pubsub_bad_topic) {
				const message = {
					table: table.id,
					error: error.message,
					data: batch,
					schema: schema,
				};
				const pubsubResult = await pubsub.publish(message, pubsub_bad_topic);
				log(`[BIGQUERY] insert error; published to topic ${pubsub_bad_topic}; message id: ${pubsubResult?.messageId}`);
			}

			else {
				throw error;
			}
			
		}



	}

	log("[BIGQUERY] Data insertion complete.");
	return { ...result };
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
 * drops all 3 mixpanel tables... this is a destructive operation
 * @param  {TableNames} tableNames
 */
async function dropTables(tableNames) {
	log("[BIGQUERY] Dropping all tables...");
	const [allTables] = await client.dataset(bigquery_dataset).getTables();
	const droppedTables = [];
	const targetTables = Object.values(tableNames);
	// @ts-ignore
	const tablesToDrop = allTables.filter((table) => targetTables.includes(table.id));
	const dropPromises = tablesToDrop.map(async (table) => {
		droppedTables.push(table.id);
		const [tableDropResult] = await table.delete();
	});
	const result = await Promise.all(dropPromises);
	log(`[BIGQUERY] Dropped ${droppedTables.length} tables: ${droppedTables.join(", ")}`);
	return { numTablesDropped: droppedTables.length, tablesDropped: droppedTables };

}

main.drop = dropTables;
main.init = initializeBigQuery;
module.exports = main;


