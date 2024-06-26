/*
----
AMAZON S3 MIDDLEWARE
----
*/

const { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectsCommand, CreateBucketCommand, HeadBucketCommand } = require('@aws-sdk/client-s3');
const path = require('path');
const { uid, touch, rm, load } = require('ak-tools');
const { tmpdir } = require('os');
const log = require("../components/logger.js");
const dayjs = require('dayjs');
const TODAY = dayjs().format('YYYY-MM-DD');
const zlib = require('zlib');
const { insertWithRetry } = require("../components/retries.js");

const NODE_ENV = process.env.NODE_ENV || "prod";
const TEMP_DIR = NODE_ENV === 'prod' ? path.resolve(tmpdir()) : path.resolve('./tmp');
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

// These vars should be cached and only run once when the server starts
/** @type {S3Client} */
let s3client;
let s3_bucket;
let s3_region;
let s3_access_key_id;
let s3_secret_access_key;
let isClientReady;
let isBucketReady;
let canWriteToBucket;

/**
 * Main function to handle S3 data insertion
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
	const init = await initializeS3(tableNames);
	if (!init.every(i => i)) throw new Error("Failed to initialize S3 middleware.");
	const { eventTable, userTable, groupTable } = tableNames;

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

	const result = await insertWithRetry(insertData, data, targetPrefix);
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;
}

async function initializeS3(tableNames) {
	// ENV STUFF
	({
		s3_bucket,
		s3_region,
		s3_access_key_id,
		s3_secret_access_key,
	} = process.env);

	if (!isClientReady) {
		isClientReady = await verifyS3Credentials();
		if (!isClientReady) throw new Error("S3 credentials verification failed.");
	}

	if (!isBucketReady) {
		isBucketReady = await verifyOrCreateBucket();
		if (!isBucketReady) throw new Error("S3 bucket does not exist.");
	}


	if (!canWriteToBucket) {
		canWriteToBucket = await verifyReadAndWritePermissions();
		if (!canWriteToBucket) throw new Error("Could not verify read/write bucket permissions.");
	}

	return [isClientReady, isBucketReady, canWriteToBucket];
}

async function verifyS3Credentials() {
	log("[S3] Verifying S3 credentials...");
	s3client = new S3Client({
		region: s3_region,
		credentials: {
			accessKeyId: s3_access_key_id,
			secretAccessKey: s3_secret_access_key
		}
	});

	try {
		const listObjectResult = await s3client.send(new ListObjectsV2Command({ Bucket: s3_bucket }));
		log("[S3] credentials verified.");
		return true;
	} catch (error) {
		log("[S3] Error verifying S3 credentials:", error);
		return error.message;
	}
}


async function verifyOrCreateBucket() {
	log("[S3] Verifying or creating S3 bucket...");

	// Check if the bucket exists
	try {
		const checkForBucket = await s3client.send(new HeadBucketCommand({ Bucket: s3_bucket }));
		log(`[S3] Bucket ${s3_bucket} already exists.`);
		return true;
	} catch (error) {
		if (error.name === 'NotFound') {
			log(`[S3] Bucket ${s3_bucket} does not exist. Creating...`);
		} else {
			log(`[S3] Error checking bucket existence: ${error.message}`);
			return false;
		}
	}

	// Create the bucket if it does not exist
	try {
		const createBucketCommand = new CreateBucketCommand({ Bucket: s3_bucket });
		const bucketCreateCommand = await s3client.send(createBucketCommand);
		log(`[S3] Bucket ${s3_bucket} created.`);
		return true;
	} catch (error) {
		log(`[S3] Failed to create bucket ${s3_bucket}: ${error.message}`);
		return false;
	}
}

/**
 * Verify read and write permissions to the bucket
 * we will write a dummy file to the bucket and then read it back
 * everything gets deleted at the end
 */
async function verifyReadAndWritePermissions() {
	log("[S3] Verifying read/write permissions...");
	const dummyFileName = "dummy-s3.txt";
	const dummyDownloadFileName = "dummy-s3-download.txt";
	const FILE_PATH = path.resolve(TEMP_DIR, dummyFileName);
	const FILE_PATH_DOWNLOAD = path.resolve(TEMP_DIR, dummyDownloadFileName);
	const localFileWriteResult = await touch(FILE_PATH, 'hello!');

	if (!localFileWriteResult) {
		log("[S3] Failed to write dummy file to local disk.");
		return false;
	}

	try {
		// Upload to S3
		log("[S3] Uploading dummy file to bucket...");
		const dummyUpload = await s3client.send(new PutObjectCommand({
			Bucket: s3_bucket,
			Key: dummyFileName,
			Body: await load(FILE_PATH)
		}));
		log("[S3] Upload successful.");

		// Download from S3
		log("[S3] Downloading dummy file from bucket...");
		const data = await s3client.send(new GetObjectCommand({
			Bucket: s3_bucket,
			Key: dummyFileName
		}));
		// @ts-ignore
		const bodyContents = await streamToString(data.Body);
		const createDownloadResult = await touch(FILE_PATH_DOWNLOAD, bodyContents);
		log("[S3] Download successful.");

		// Verify contents
		log("[S3] Verifying downloaded file contents...");
		const localFileContents = await load(FILE_PATH);
		const downloadedContents = await load(FILE_PATH_DOWNLOAD);
		if (downloadedContents !== localFileContents) {
			log("[S3] Downloaded file contents do not match local file contents.");
			return false;
		}
		log("[S3] Downloaded file contents match local file contents.");

		// Delete from S3
		log("[S3] Deleting dummy file from bucket...");
		const deleteResult = await s3client.send(new DeleteObjectsCommand({
			Bucket: s3_bucket,
			Delete: {
				Objects: [{ Key: dummyFileName }]
			}
		}));
		log("[S3] Delete successful.");

		// Delete local files
		log("[S3] Deleting local files...");
		const localDeleteResult = await rm(FILE_PATH);
		const localDownloadDeleteResult = await rm(FILE_PATH_DOWNLOAD);
		log("[S3] Local files deleted.");

	} catch (error) {
		log("[S3] Error verifying read/write permissions:", error);
		return false;
	}

	log("[S3] Read/write permissions verified.");
	return true;
}

/**
 * Insert data into S3
 * @param  {FlatData} batch
 * @param  {string} prefix
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, prefix) {
	log("[S3] Starting data upload...");
	if (!prefix) throw new Error("Prefix name not provided.");
	if (prefix?.endsWith("/")) prefix = prefix.slice(0, -1);
	let result = { status: "born" };
	const fileName = `${prefix}/${TODAY}_${uid(42)}.json.gz`;
	const dataToUpload = zlib.gzipSync(batch.map(record => JSON.stringify(record)).join('\n'));
	// const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');

	try {
		const insertResult = await s3client.send(new PutObjectCommand({
			Bucket: s3_bucket,
			Key: fileName,
			Body: dataToUpload
		}));
		result = { status: "success", insertedRows: batch.length, failedRows: 0 };
	} catch (error) {
		log(`[S3] Error uploading data to S3: ${error.message}`, error);
		throw error;
	}

	log("[S3] Data insertion complete.");
	return result;
}

/**
 * Delete bucket and all files. This is a destructive operation.
 * @param  {TableNames} tableNames
 */
async function deleteAllFiles(tableNames) {
	const { eventTable, userTable, groupTable } = tableNames;
	const tables = [eventTable, userTable, groupTable];
	try {
		const listParams = {
			Bucket: s3_bucket,
		};
		const listObjectsResponse = await s3client.send(new ListObjectsV2Command(listParams));
		const filesToDelete = listObjectsResponse?.Contents?.filter(f => tables.some(t => f?.Key?.includes(t)));

		const deleteParams = {
			Bucket: s3_bucket,
			Delete: {
				Objects: filesToDelete?.map(f => ({ Key: f.Key }))
			}
		};
		if (!filesToDelete || !filesToDelete?.length) { return { numFilesDeleted: 0 }; }
		const deleteFilesResult = await s3client.send(new DeleteObjectsCommand(deleteParams));
		log(`[S3] Deleted ${filesToDelete?.length || 0} files from bucket ${s3_bucket}.`);
		return { numFilesDeleted: filesToDelete?.length || 0 };
	} catch (error) {
		log(`[S3] Error deleting files from S3: ${error.message}`, error);
		throw error;
	}


}

/**
 * Helper function to convert a stream to a string
 * @param {ReadableStream} stream
 * @returns {Promise<string>}
 */
async function streamToString(stream) {
	return new Promise((resolve, reject) => {
		const chunks = [];

		// @ts-ignore
		stream.on("data", (chunk) => chunks.push(chunk));
		// @ts-ignore
		stream.on("error", reject);
		// @ts-ignore
		stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
	});
}

main.drop = deleteAllFiles;
main.init = initializeS3;
module.exports = main;
