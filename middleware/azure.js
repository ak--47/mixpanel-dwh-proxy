/*
----
AZURE BLOB STORAGE MIDDLEWARE
----
*/

const { BlobServiceClient, ContainerClient } = require('@azure/storage-blob');
const path = require('path');
const { uid, touch, rm, load } = require('ak-tools');
const { tmpdir } = require('os');
const log = require("../components/logger.js");
const dayjs = require('dayjs');
const TODAY = dayjs().format('YYYY-MM-DD');

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
/** @type {BlobServiceClient} */
let blobServiceClient;
let containerClient;
let azure_account_name;
let azure_account_key;
let azure_container_name;
let isClientReady;
let canWriteToContainer;

/**
 * Main function to handle Azure Blob Storage data insertion
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
	const init = await initializeAzureBlobStorage(tableNames);
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

	const result = await insertData(data, targetPrefix);
	const duration = Date.now() - startTime;
	result.duration = duration;
	return result;
}

async function initializeAzureBlobStorage(tableNames) {
	// ENV STUFF
	({
		azure_account_name,
		azure_account_key,
		azure_container_name,
	} = process.env);

	if (!isClientReady) {
		isClientReady = await verifyAzureBlobStorageCredentials();
		if (!isClientReady) throw new Error("Azure Blob Storage credentials verification failed.");
	}

	if (!canWriteToContainer) {
		canWriteToContainer = await verifyOrCreateContainer();
		if (!canWriteToContainer) throw new Error("Could not verify read/write container permissions.");
	}

	return [isClientReady];
}

async function verifyAzureBlobStorageCredentials() {
	log("Verifying Azure Blob Storage credentials...");
	const credentials = new StorageSharedKeyCredential(azure_account_name, azure_account_key);
	const pipeline = newPipeline(credentials);
	blobServiceClient = new BlobServiceClient(`https://${azure_account_name}.blob.core.windows.net`, pipeline);
	containerClient = blobServiceClient.getContainerClient(azure_container_name);

	try {
		await blobServiceClient.getAccountInfo();
		log("Azure Blob Storage credentials verified.");
		return true;
	} catch (error) {
		log("Error verifying Azure Blob Storage credentials:", error);
		return error.message;
	}
}

async function verifyOrCreateContainer() {
	log("Verifying or creating Azure Blob Storage container...");

	try {
		await containerClient.getProperties();
		log(`Container ${azure_container_name} already exists.`);
		return true;
	} catch (error) {
		if (error.statusCode === 404) {
			log(`Container ${azure_container_name} does not exist. Creating...`);
		} else {
			log(`Error checking container existence: ${error.message}`);
			return false;
		}
	}

	try {
		await containerClient.create();
		log(`Container ${azure_container_name} created.`);
		return true;
	} catch (error) {
		log(`Failed to create container ${azure_container_name}: ${error.message}`);
		return false;
	}
}

/**
 * Verify read and write permissions to the container
 * we will write a dummy file to the container and then read it back
 * everything gets deleted at the end
 */
async function verifyReadAndWritePermissions() {
	log("Verifying read/write permissions...");
	const dummyFileName = "dummy.txt";
	const dummyDownloadFileName = "dummy-download.txt";
	const FILE_PATH = path.resolve(TEMP_DIR, dummyFileName);
	const FILE_PATH_DOWNLOAD = path.resolve(TEMP_DIR, dummyDownloadFileName);
	const localFileWriteResult = await touch(FILE_PATH, 'hello!');

	if (!localFileWriteResult) {
		log("Failed to write dummy file to local disk.");
		return false;
	}

	try {
		// Upload to Azure Blob Storage
		log("Uploading dummy file to container...");
		const blockBlobClient = containerClient.getBlockBlobClient(dummyFileName);
		await blockBlobClient.uploadFile(FILE_PATH);
		log("Upload successful.");

		// Download from Azure Blob Storage
		log("Downloading dummy file from container...");
		await blockBlobClient.downloadToFile(FILE_PATH_DOWNLOAD);
		log("Download successful.");

		// Verify contents
		log("Verifying downloaded file contents...");
		const localFileContents = await load(FILE_PATH);
		const downloadedContents = await load(FILE_PATH_DOWNLOAD);
		if (downloadedContents !== localFileContents) {
			log("Downloaded file contents do not match local file contents.");
			return false;
		}
		log("Downloaded file contents match local file contents.");

		// Delete from Azure Blob Storage
		log("Deleting dummy file from container...");
		await blockBlobClient.delete();
		log("Delete successful.");

		// Delete local files
		log("Deleting local files...");
		await rm(FILE_PATH);
		await rm(FILE_PATH_DOWNLOAD);
		log("Local files deleted.");

	} catch (error) {
		log("Error verifying read/write permissions:", error);
		return false;
	}

	log("Read/write permissions verified.");
	return true;
}

/**
 * Insert data into Azure Blob Storage
 * @param  {FlatData} batch
 * @param  {string} prefix
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, prefix) {
	log("Starting data upload...\n");
	if (!prefix) throw new Error("Prefix name not provided.");
	if (prefix?.endsWith("/")) prefix = prefix.slice(0, -1);
	let result = { status: "born" };
	const fileName = `${prefix}/${TODAY}_${uid(42)}.json.gz`;
	const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');

	try {
		const blockBlobClient = containerClient.getBlockBlobClient(fileName);
		await blockBlobClient.upload(dataToUpload, dataToUpload.length, { blobHTTPHeaders: { blobContentEncoding: 'gzip' } });
		result = { status: "success", insertedRows: batch.length, failedRows: 0 };
	} catch (error) {
		log(`Error uploading data to Azure Blob Storage: ${error.message}`, error);
		throw error;
	}

	log("\n\tData insertion complete.\n");
	return result;
}

/**
 * Delete container and all blobs. This is a destructive operation.
 * @param  {TableNames} tableNames
 */
async function deleteAllFiles(tableNames) {
	const { eventTable, userTable, groupTable } = tableNames;
	const tables = [eventTable, userTable, groupTable];
	try {
		const listBlobsResponse = await containerClient.listBlobsFlat();
		const blobsToDelete = [];
		for await (const blob of listBlobsResponse) {
			if (tables.some(t => blob.name.includes(t))) {
				blobsToDelete.push({ name: blob.name });
			}
		}
		const deletePromises = blobsToDelete.map(blob => containerClient.getBlockBlobClient(blob.name).delete());
		await Promise.all(deletePromises);
		log(`Deleted ${blobsToDelete.length} blobs from container ${azure_container_name}.`);
	} catch (error) {
		log(`Error deleting blobs from Azure Blob Storage: ${error.message}`, error);
		throw error;
	}
}

main.drop = deleteAllFiles;
main.init = initializeAzureBlobStorage;
module.exports = main;
