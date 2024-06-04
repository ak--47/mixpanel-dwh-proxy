/*
----
AZURE BLOB STORAGE MIDDLEWARE
----
*/

const { BlobServiceClient, ContainerClient, StorageSharedKeyCredential } = require('@azure/storage-blob');
const path = require('path');
const { uid, touch, rm, load } = require('ak-tools');
const { tmpdir } = require('os');
const log = require("../components/logger.js");
const dayjs = require('dayjs');
const TODAY = dayjs().format('YYYY-MM-DD');
const { gzipSync } = require('zlib');
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
/** @type {BlobServiceClient} */
let blobServiceClient;
/** @type {ContainerClient} */
let containerClient;
let azure_account;
let azure_key;
let azure_connection_string;
let azure_container;
let isClientReady;
let isContainerReady;
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
	if (!init.every(i => i)) throw new Error("Failed to initialize Azure Blob Storage middleware.");
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
		azure_account,
		azure_key,
		azure_container,
		azure_connection_string
	} = process.env);

	if (!isClientReady) {
		isClientReady = await verifyAzureBlobStorageCredentials();
		if (!isClientReady) throw new Error("Azure Blob Storage credentials verification failed.");
	}

	if (!isContainerReady) {
		isContainerReady = await verifyOrCreateContainer();
		if (!isContainerReady) throw new Error("Could not verify or create blob container");
	}

	if (!canWriteToContainer) {
		canWriteToContainer = await verifyReadAndWritePermissions();
		if (!canWriteToContainer) throw new Error("Could not verify read/write container permissions.");
	}

	return [isClientReady, isContainerReady, canWriteToContainer];
}

async function verifyAzureBlobStorageCredentials() {
	log("[AZURE]: Verifying Azure Blob Storage credentials...");

	// name + key auth
	if (azure_account && azure_key) {
		log("[AZURE]: Using Azure Blob Storage account name and key for authentication.");
		const credentials = new StorageSharedKeyCredential(azure_account, azure_key);
		blobServiceClient = new BlobServiceClient(`https://${azure_account}.blob.core.windows.net`, credentials);
		containerClient = blobServiceClient.getContainerClient(azure_container);
	}

	// connection string auth
	else if (azure_connection_string && azure_account) {
		log("[AZURE]: Using Azure Blob Storage connection string for authentication.");
		blobServiceClient = BlobServiceClient.fromConnectionString(azure_connection_string);
		containerClient = blobServiceClient.getContainerClient(azure_container);
	}


	try {
		await blobServiceClient.getAccountInfo();
		log("[AZURE]: Azure Blob Storage credentials verified.");
		return true;
	} catch (error) {
		log("[AZURE]: Error verifying Azure Blob Storage credentials:", error);
		return error.message;
	}
}

async function verifyOrCreateContainer() {
	log("[AZURE]: Verifying or creating Azure Blob Storage container...");

	try {
		const containerProperties = await containerClient.getProperties();
		log(`[AZURE]: Container ${azure_container} already exists.`);
		return true;
	} catch (error) {
		if (error.statusCode === 404) {
			log(`[AZURE]: Container ${azure_container} does not exist. Creating...`);
		} else {
			log(`[AZURE]: Error checking container existence: ${error.message}`);
			return false;
		}
	}

	try {
		const containerCreateResult = await containerClient.create();
		log(`[AZURE]: Container ${azure_container} created.`);
		return true;
	} catch (error) {
		log(`[AZURE]: Failed to create container ${azure_container}: ${error.message}`);
		return false;
	}
}

/**
 * Verify read and write permissions to the container
 * we will write a dummy file to the container and then read it back
 * everything gets deleted at the end
 */
async function verifyReadAndWritePermissions() {
	log("[AZURE]: Verifying read/write permissions...");
	const dummyFileName = "dummy-azure.txt";
	const dummyDownloadFileName = "dummy-azure-download.txt";
	const FILE_PATH = path.resolve(TEMP_DIR, dummyFileName);
	const FILE_PATH_DOWNLOAD = path.resolve(TEMP_DIR, dummyDownloadFileName);
	const localFileWriteResult = await touch(FILE_PATH, 'hello!');

	if (!localFileWriteResult) {
		log("[AZURE]: Failed to write dummy file to local disk.");
		return false;
	}

	try {
		// Upload to Azure Blob Storage
		log("[AZURE]: Uploading dummy file to container...");
		const blockBlobClient = containerClient.getBlockBlobClient(dummyFileName);
		const dummyUploadResult = await blockBlobClient.uploadFile(FILE_PATH);
		log("[AZURE]: Upload successful.");

		// Download from Azure Blob Storage
		log("[AZURE]: Downloading dummy file from container...");
		const dummyDownloadResult = await blockBlobClient.downloadToFile(FILE_PATH_DOWNLOAD);
		log("[AZURE]: Download successful.");

		// Verify contents
		log("[AZURE]: Verifying downloaded file contents...");
		const localFileContents = await load(FILE_PATH);
		const downloadedContents = await load(FILE_PATH_DOWNLOAD);
		if (downloadedContents !== localFileContents) {
			log("[AZURE]: Downloaded file contents do not match local file contents.");
			return false;
		}
		log("[AZURE]: Downloaded file contents match local file contents.");

		// Delete from Azure Blob Storage
		log("[AZURE]: Deleting dummy file from container...");
		const dummyDeleteResult = await blockBlobClient.delete();
		log("[AZURE]: Delete successful.");

		// Delete local files
		log("[AZURE]: Deleting local files...");
		const locallyCreatedDeleteResult = await rm(FILE_PATH);
		const locallyDownloadedDeleteResult = await rm(FILE_PATH_DOWNLOAD);
		log("[AZURE]: Local files deleted.");

	} catch (error) {
		log("[AZURE]: Error verifying read/write permissions:", error);
		return false;
	}

	log("[AZURE]: Read/write permissions verified.");
	return true;
}

/**
 * Insert data into Azure Blob Storage
 * @param  {FlatData} batch
 * @param  {string} prefix
 * @return {Promise<InsertResult>}
 */
async function insertData(batch, prefix) {
	log("[AZURE]: Starting data upload...");
	if (!prefix) throw new Error("Prefix name not provided.");
	if (prefix?.endsWith("/")) prefix = prefix.slice(0, -1);
	let result = { status: "born" };
	const fileName = `${prefix}/${TODAY}_${uid(42)}.json`;
	const dataToUpload = batch.map(record => JSON.stringify(record)).join('\n');
	/** @type {import('@azure/storage-blob').BlockBlobUploadOptions} */
	const options = { blobHTTPHeaders: { blobContentType: 'application/json' } };
	try {
		const blockBlobClient = containerClient.getBlockBlobClient(fileName);
		const uploadResult = await blockBlobClient.upload(dataToUpload, dataToUpload.length, options);
		result = { status: "success", insertedRows: batch.length, failedRows: 0 };
	} catch (error) {
		log(`[AZURE]: Error uploading data to Azure Blob Storage: ${error.message}`, error);
		throw error;
	}

	log("[AZURE]: Data insertion complete.");
	return result;
}

/**
 * Delete container and all blobs. This is a destructive operation.
 * @param  {TableNames} tableNames
 */
async function deleteAllFiles(tableNames) {
	log("[AZURE]: Deleting all blobs from container...");
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
		const deletePromiseResults = await Promise.all(deletePromises);
		log(`[AZURE]: Deleted ${blobsToDelete.length} blobs from container ${azure_container}.`);
		return { numDeleted: blobsToDelete.length };
	} catch (error) {
		log(`[AZURE]: Error deleting blobs from Azure Blob Storage: ${error.message}`, error);
		throw error;
	}


}

main.drop = deleteAllFiles;
main.init = initializeAzureBlobStorage;
module.exports = main;
