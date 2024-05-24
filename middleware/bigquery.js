/*
----
BIQUERY MIDDLEWARE
----
*/
const NODE_ENV = process.env.NODE_ENV || 'prod';
const { BigQuery } = require('@google-cloud/bigquery');
const u = require('ak-tools');
const { prepHeaders, cleanName, checkEnv } = require('../components/inference');
const log = require('../components/logger.js');
require('dotenv').config();

let client;  // use application default credentials; todo: override with service account
let datasetId = '';
let tableId = '';

/** @typedef { import('../types.js').BigQueryTypes } BQTypes */

/**
* BigQuery middleware
* implements this contract
* @param  {import('../types.js').Schema} schema
* @param  {import('../types.js').csvBatch[]} batches
* @param  {import('../types.js').JobConfig} PARAMS
* @returns {Promise<import('../types.js').WarehouseUploadResult>}
*/
async function loadToBigQuery(schema, batches, PARAMS) {
	let {
		bigquery_dataset = '',
		bigquery_project = '',
		bigquery_keyfile = '',
		bigquery_service_account = '',
		bigquery_service_account_pass = '',
		dry_run = false,
		table_name = ''
	} = PARAMS;

	if (!bigquery_dataset) throw new Error('bigquery_dataset is required');
	if (!table_name) throw new Error('table_name is required');
	if (!bigquery_project) throw new Error('bigquery_project is required');
	if (!bigquery_keyfile && (!bigquery_service_account || !bigquery_service_account_pass)) throw new Error('bigquery_keyfile or bigquery_service_account + bigquery_service_account_pass is required');
	if (!schema) throw new Error('schema is required');
	if (!batches) throw new Error('batches is required');
	if (batches.length === 0) throw new Error('batches is empty');

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
			private_key: bigquery_service_account_pass
		};
	}

	client = new BigQuery(auth);

	const validCredentials = await verifyBigQueryCredentials();
	if (typeof validCredentials === 'boolean' && !validCredentials) throw new Error(`Invalid BigQuery credentials; Got Message: ${validCredentials}`);

	table_name = cleanName(table_name);
	datasetId = bigquery_dataset;
	tableId = table_name;
	// ensure column headers are clean in schema and batches
	const columnHeaders = schema.map(field => field.name);
	const headerMap = prepHeaders(columnHeaders);
	const headerReplacePairs = prepHeaders(columnHeaders, true);
	// @ts-ignore
	schema = schema.map(field => u.rnVals(field, headerReplacePairs));
	batches = batches.map(batch => batch.map(row => u.rnKeys(row, headerMap)));

	// build a specific schema for BigQuery
	// @ts-ignore
	schema = schemaToBQS(schema);

	if (dry_run) {
		log('Dry run requested. Skipping BigQuery upload.');
		return { schema, dataset: datasetId, table: tableId, upload: [], logs: log.getLog() };
	}

	// do work
	const dataset = await createDataset();
	const table = await createTable(schema);
	const upload = await insertData(schema, batches);
	const logs = log.getLog();

	const uploadJob = { schema, dataset, table, upload, logs };

	// @ts-ignore
	return uploadJob;
}


async function createDataset() {
	const datasets = await client.getDatasets();
	const datasetExists = datasets[0].some(dataset => dataset.id === datasetId);

	if (!datasetExists) {
		const [dataset] = await client.createDataset(datasetId);
		log(`Dataset ${dataset.id} created.\n`);
		return datasetId;
	} else {
		log(`Dataset ${datasetId} already exists.\n`);
		return datasetId;
	}
}

/**
 * @param  {import('../types.js').Schema} schema
 */
function schemaToBQS(schema) {
	const transformedSchema = schema.map(field => {
		let bqType;
		let mode = 'NULLABLE';

		// Determine BigQuery type
		// ? https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
		switch (field.type.toUpperCase()) {
			// JSON TYPES
			case 'OBJECT':
				bqType = 'JSON';

				break;
			case 'ARRAY':
				bqType = 'JSON';

				break;
			case 'JSON':
				bqType = 'JSON';

				break;

			// NUMERIC TYPES
			case 'INT':
				bqType = 'INT64';
				break;
			case 'FLOAT':
				bqType = 'FLOAT64';
				break;

			// DATE TYPES
			case 'DATE':
				bqType = 'DATE';
				break;
			case 'TIMESTAMP':
				bqType = 'TIMESTAMP';
				break;

			// PRIMITIVE TYPES
			case 'BOOLEAN':
				bqType = 'BOOLEAN';
				break;
			case 'STRING':
				bqType = 'STRING';
			default:
				bqType = field.type.toUpperCase();
				break;
		}

		// Build field definition
		const fieldSchema = {
			name: field.name,
			type: bqType,
			mode
		};



		return fieldSchema;
	});

	return transformedSchema;
}
/**
 * @param  {any} value
 * @param  {BQTypes} type
 */
function convertField(value, type) {
	switch (type) {
		case 'STRING':
			return value?.toString();
		case 'TIMESTAMP':
			return value;
		case 'DATE':
			return value;
		case 'INT64':
			return parseInt(value);
		case 'FLOAT64':
			return parseFloat(value);
		case 'BOOLEAN':
			if (typeof value === 'boolean') return value;
			if (typeof value === 'number') return value === 1;
			if (typeof value === 'string') return value?.toLowerCase() === 'true';
		case 'RECORD':
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === 'object') return JSON.stringify(value);
			if (typeof value === 'string') return value;
		case 'JSON':
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === 'object') return JSON.stringify(value);
			if (typeof value === 'string') return value;
		case 'STRUCT':
			if (Array.isArray(value)) return JSON.stringify(value);
			if (typeof value === 'object') return JSON.stringify(value);
			if (typeof value === 'string') return value;
		default:
			return value;
	}
}

function prepareRowsForInsertion(batch, schema) {
	return batch.map(row => {
		const newRow = {};
		schema.forEach(field => {
			//sparse CSVs will have missing fields
			if (row[field.name] !== '') {
				try {
					newRow[field.name] = convertField(row[field.name], field.type.toUpperCase());
				}
				catch (error) {
					debugger;
				}
			}
			if (row[field.name] === '') delete newRow[field.name];
			if (row[field.name] === undefined) delete newRow[field.name];
			if (row[field.name] === null) delete newRow[field.name];
		});
		return newRow;
	});
}

async function waitForTableToBeReady(table, retries = 20, maxInsertAttempts = 20) {
	log('Checking if table exits...');

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

	log('\nChecking if table is ready for operations...');
	let insertAttempt = 0;
	while (insertAttempt < maxInsertAttempts) {
		try {
			// Attempt a dummy insert that SHOULD fail, but not because 404
			const dummyRecord = { [u.uid()]: u.uid() };
			await table.insert([dummyRecord]);
			log('...should never get here...');
			return true; // If successful, return true immediately
		} catch (error) {
			if (error.code === 404) {
				const sleepTime = u.rand(1000, 5000);
				log(`\tTable not ready for operations, sleeping ${u.prettyTime(sleepTime)} retrying... attempt #${insertAttempt + 1}`);
				await u.sleep(sleepTime);
				insertAttempt++;
			}

			else if (error.name === 'PartialFailureError') {
				log('\tTable is ready for operations\n');
				return true;
			}

			else {
				log('should never get here either');
				debugger;
			}



		}
	}
	return false; // Return false if all attempts fail
}

async function insertData(schema, batches) {
	const table = client.dataset(datasetId).table(tableId);

	// Check if table is ready
	const tableReady = await waitForTableToBeReady(table);
	if (!tableReady) {
		log('\nTable is NOT ready after all attempts. Aborting data insertion.');
		process.exit(1);
	}
	log('Starting data insertion...\n');
	const results = [];

	/** @type {import('@google-cloud/bigquery').InsertRowsOptions} */
	const options = {
		skipInvalidRows: false,
		ignoreUnknownValues: false,
		raw: false,
		partialRetries: 3,
		schema: schema,
	};
	// Continue with data insertion if the table is ready
	let currentBatch = 0;
	for (const batch of batches) {
		currentBatch++;
		const start = Date.now();
		try {
			const rows = prepareRowsForInsertion(batch, schema);
			const [response] = await table.insert(rows, options);
			const duration = Date.now() - start;
			results.push({ status: 'success', insertedRows: rows.length, failedRows: 0, duration });
			if (log.isVerbose()) u.progress(`\tsent batch ${u.comma(currentBatch)} of ${u.comma(batches.length)} batches`);

		} catch (error) {
			const duration = Date.now() - start;
			if (error.name === 'PartialFailureError') {
				const failedRows = error.errors.length;
				const insertedRows = batch.length - failedRows;
				const uniqueErrors = Array.from(new Set(error.errors.map(e => e.errors.map(e => e.message)).flat()));;
				results.push({ status: 'error', type: "partial failure", failedRows, insertedRows, errors: uniqueErrors, duration });
				log(`Partial failure inserting batch ${currentBatch}; will continue`);
			}
			batch;
			debugger;
			throw error;
		}
	}
	log('\n\tData insertion complete.\n');
	return results;
}

async function createTable(schema) {
	const dataset = client.dataset(datasetId);
	const table = dataset.table(tableId);
	const [tableExists] = await table.exists();

	if (tableExists) {
		log(`Table ${tableId} already exists. Deleting existing table.`);
		await table.delete();
		log(`Table ${tableId} has been deleted.`);
	}

	// Proceed to create a new table with the new schema
	const options = { schema: schemaToBQS(schema) };
	const [newTable] = await dataset.createTable(tableId, options);
	log(`New table ${newTable.id} created with the updated schema.\n`);
	const newTableExists = await newTable.exists();
	return newTable.id;
}

async function verifyBigQueryCredentials() {
	const query = 'SELECT 1';
	try {
		const [rows] = await client.query(query);
		log('BigQuery credentials are valid');
		return true;
	} catch (error) {
		log('Error verifying BigQuery credentials:', error);
		return error.message;
	}
}




module.exports = loadToBigQuery;