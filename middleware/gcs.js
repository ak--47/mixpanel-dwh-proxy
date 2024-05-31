/*
----
GOOGLE CLOUD STORAGE MIDDLEWARE
----
*/

const { Storage } = require('@google-cloud/storage');

const NODE_ENV = process.env.NODE_ENV || "prod";
const u = require("ak-tools");
const log = require("../components/logger.js");
const path = require('path');
const { uid, touch, rm, load } = require('ak-tools');
const { tmpdir } = require('os');
const TEMP_DIR = NODE_ENV === 'prod' ? path.resolve(tmpdir()) : path.resolve('./tmp');
const dayjs = require('dayjs');
const TODAY = dayjs().format('YYYY-MM-DD');
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
/** @type {Storage} */
let client;
let gcs_project;
let gcs_bucket;
let gcs_service_account;
let gcs_service_account_private_key;
let gcs_keyfile;
let isClientReady;
let isBucketReady;
let canWriteToBucket;




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
	const init = await initializeGoogleStorage(tableNames);
	if (!init.every(i => i)) throw new Error("Failed to initialize Google Cloud Storage middleware.");
	const { eventTable, userTable, groupTable } = tableNames;
	// now we know the tables is ready and we can insert data; this runs repeatedly
	let targetPrefix;
	switch (type) {
		case "track":
			targetPrefix = eventTable;
			break;
		case "engage":
			targetPrefix = userTable;
			break;
		case "groups":
			targetPrefix = groupTable;
			break;
		default:
			throw new Error("Invalid Record Type");
	}


	// @ts-ignore
	const result = await insertData(data, targetPrefix);
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;

}

async function initializeGoogleStorage(tableNames) {
	// ENV STUFF
	({
		gcs_project,
		gcs_bucket,
		gcs_service_account,
		gcs_service_account_private_key,
		gcs_keyfile
	} =
		process.env);

	const { eventTable, userTable, groupTable } = tableNames;

	if (!isClientReady) {
		isClientReady = await verifyGcsCredentials();
		if (!isClientReady) throw new Error("BigQuery credentials verification failed.");
	}

	if (!isBucketReady) {
		isBucketReady = await verifyOrCreateBucket();
		if (!isBucketReady) throw new Error("Dataset verification or creation failed.");
	}

	if (!canWriteToBucket) {
		canWriteToBucket = await verifyReadAndWritePermissions();
		if (!canWriteToBucket) throw new Error("could not verify read/write bucket permissions.");
	}

	return [isClientReady];
}

async function verifyGcsCredentials() {

	// credentials or keyfile or application default credentials
	/** @type {import('@google-cloud/bigquery').BigQueryOptions} */
	const auth = {};
	if (gcs_keyfile) {
		auth.keyFilename = gcs_keyfile;
		auth.keyFile = gcs_keyfile;
	}
	if (gcs_project) auth.projectId = gcs_project;
	if (gcs_service_account && gcs_service_account_private_key) {
		auth.credentials = {
			client_email: gcs_service_account,
			private_key: gcs_service_account_private_key,
		};
	}

	client = new Storage(auth);


	try {
		const [serviceAccount] = await client.getServiceAccount({});
		log("[GCS] credentials verified.");
		return true;
	} catch (error) {
		log("[GCS] Error verifying BigQuery credentials:", error);
		return error.message;
	}
}

async function verifyOrCreateBucket() {
	const [buckets] = await client.getBuckets();
	const bucketExists = buckets.some((b) => b.name === gcs_bucket);
	if (!bucketExists) {
		log(`Bucket ${gcs_bucket} does not exist. creating...`);
		const [newBucket] = await client.createBucket(gcs_bucket);
		const [moreBuckets] = await client.getBuckets();
		const moreBucketExists = moreBuckets.some((b) => b.name === gcs_bucket);
		if (!moreBucketExists) {
			log(`Failed to create bucket ${gcs_bucket}`);
			return false;
		} else {
			log(`Bucket ${gcs_bucket} created.`);
			return true;
		}
	} else {
		log(`Bucket ${gcs_bucket} already exists.`);
		return true;
	}
}

/**
 * verify read and write permissions to the bucket
 * we will write a dummy file to the bucket and then read it back
 * everything gets deleted at the end
 */
async function verifyReadAndWritePermissions() {
	log("[GCS] Verifying read/write permissions...");
	const dummyFileName = "dummy-gcs.txt";
	const dummyDownloadFileName = "dummy-download-gcs.txt";
	const FILE_PATH = path.resolve(TEMP_DIR, dummyFileName);
	const FILE_PATH_DOWNLOAD = path.resolve(TEMP_DIR, dummyDownloadFileName);
	const localFileWriteResult = await touch(FILE_PATH, 'hello!');

	if (!localFileWriteResult) {
		log("[GCS] Failed to write dummy file to local disk.");
		return false;
	}
	let uploadResult;
	let downloadResult;
	let downloadedContents;
	let deleteResult;

	//upload to cloud storage
	try {
		log("[GCS] Uploading dummy file to bucket...");
		([uploadResult] = await client.bucket(gcs_bucket).upload(FILE_PATH));
		log("[GCS] Upload successful.");
	}
	catch (error) {
		log("[GCS] Error uploading dummy file to bucket:", error);
		return false;
	}

	//download from cloud storage
	try {
		log("[GCS] Downloading dummy file from bucket...");
		([downloadResult] = await client.bucket(gcs_bucket).file(dummyFileName).download({ destination: FILE_PATH_DOWNLOAD }));
		log("[GCS] Download successful.");
	}
	catch (error) {
		log("[GCS] Error downloading dummy file from bucket:", error);
		return false;
	}

	//verify contents
	try {
		log("[GCS] Verifying downloaded file contents...");
		const localFileContents = await load(FILE_PATH);
		downloadedContents = await load(FILE_PATH_DOWNLOAD);
		if (downloadedContents !== localFileContents) {
			log("[GCS] Downloaded file contents do not match local file contents.");
			return false;
		}
		log("[GCS] Downloaded file contents match local file contents.");
	}
	catch (error) {
		log("[GCS] Error reading downloaded file contents:", error);
		return false;
	}

	//delete from cloud storage
	try {
		log("[GCS] Deleting dummy file from bucket...");
		([deleteResult] = await client.bucket(gcs_bucket).file(dummyFileName).delete());
		log("[GCS] Delete successful.");
	}
	catch (error) {
		log("[GCS] Error deleting dummy file from bucket:", error);
		return false;
	}

	//delete local file
	try {
		log("[GCS] Deleting local files...");
		const locallyMadeDeleteResult = await rm(FILE_PATH);
		const locallyDownloadedDeleteResult = await rm(FILE_PATH_DOWNLOAD);
		log("[GCS] Local files deleted.");
	}
	catch (error) {
		log("[GCS] Error deleting local files:", error);
		return false;
	}

	log("[GCS] Read/write permissions verified.");
	return true;

}


/**
 * insert data into BigQuery
 * @param  {FlatData} batch
 * @param  {string} prefix
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, prefix) {
	log("[GCS] Starting data upload...");
	if (!prefix) throw new Error("prefix name not provided.");
	if (prefix?.endsWith("/")) prefix = prefix.slice(0, -1);
	let result = { status: "born", dest: "gcs" };
	const fileName = `${prefix}/${TODAY}_${uid(42)}.json.gz`;
	const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');
	/** @type {import('@google-cloud/storage').SaveOptions} */
	const options = {
		gzip: true
	};

	try {
		const file = await client.bucket(gcs_bucket).file(fileName).save(dataToUpload, options);
		result = { status: "success", insertedRows: batch.length, failedRows: 0, dest: "gcs" };
	} catch (error) {
		debugger;
		log(`[GCS] Error uploading data to Google Cloud Storage: ${error.message}`, error);
		throw error;

	}

	log("[GCS] Data insertion complete.");
	return { ...result };
}


/**
 * delete bucket and all files this is a destructive operation
 * @param  {TableNames} tableNames
 */
async function deleteAllFiles(tableNames) {
	log("[GCS] Deleting all files from bucket...");
	const { eventTable, userTable, groupTable } = tableNames;
	const tables = [eventTable, userTable, groupTable];
	const [buckets] = await client.getBuckets();
	const bucket = buckets.find((b) => b.name === gcs_bucket);
	if (!bucket) {
		log(`[GCS] Bucket ${gcs_bucket} not found.`);
		return;
	}
	const [files] = await bucket.getFiles();
	const filesToDelete = files.filter((f) => tables.some((t) => f.name.includes(t)));
	const deletePromises = filesToDelete.map((f) => f.delete());
	const deleteResults = await Promise.all(deletePromises);
	log(`[GCS] Deleted ${deleteResults?.length} files from bucket ${gcs_bucket}.`);
	return { numFilesDeleted: deleteResults?.length };
}

main.drop = deleteAllFiles;
main.init = initializeGoogleStorage;
module.exports = main;