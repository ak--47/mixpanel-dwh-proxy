const u = require("ak-tools");
let logBuffer = [];
let verbose = false;
let cli_mode = false;
let LOG_MAX_SIZE = 1000000;

/** @typedef {import('../types').logEntry} logEntry*/

function setVerbose(value) {
	verbose = value;
}

function setCliMode(value) {
	cli_mode = value;
	verbose = value;
}

function getCliMode() {
	return cli_mode;
}

function getVerbose() {
	return verbose;
}

/**
 * @returns {logEntry[]}
 */
function getLog() {
	return logBuffer;
}

/**
 * A print function that handles logging based on the current mode and verbosity settings.
 * @param {string} message
 * @param {any} [data]
 * @param {string} [severity] - The severity label (INFO, DEBUG, ERROR).
 */
function print(message, data, severity) {
	if (verbose) {
		if (cli_mode) {
			if (data) console.log(message, data);
			else console.log(message);
		}
		if (!cli_mode) {
			// Print using structured logger with severity label
			if (data) u.sLog(message, data, severity);
			else u.sLog(message, {}, severity);
		}
	}
}

/**
 * A simple logger that can be used in both CLI and structured modes with a simple API.
 * @example
 * const log = require('./logger.js');
 * log("this is a message");
 * log("this is a message with data", {key: "value"});
 * log.verbose(false);
 * log.cli(true);
 * @param {string} message
 * @param {any} [data]
 */
function log(message, ...data) {
	if (!cli_mode) message = message?.trim();
	let props = {};

	try {
		//we have data
		if (data.length) {
			let isError = false;
			for (const item of data) {
				if (item instanceof Error) {
					isError = true;
					props.error = {
						message: item.message || "",
						stack: item.stack || "",
						name: item.name || "",
						file: item.fileName || "",
						line: item.lineNumber || ""
					};
				} else {
					Object.assign(props, item);
				}
			}

			if (isError) print(message, props, "ERROR");
			else print(message, props, "DEBUG");
		}
		// just message 
		else {
			print(message, undefined, "INFO");
		}



		// always append to log buffer
		if (message) {
			if (data.length) logBuffer.push([message, props]);
			else logBuffer.push([message]);
		}

		if (logBuffer.length > LOG_MAX_SIZE) logBuffer = logBuffer.slice(logBuffer.length / 2);
	}

	catch (e) {
		console.error("Logging error:", e);
		// SHOULD NEVER BE HERE
		debugger;
	}
}


log.verbose = setVerbose;
log.isVerbose = getVerbose;
log.getLog = getLog;
log.cli = setCliMode;
log.isCli = getCliMode;
module.exports = log;
