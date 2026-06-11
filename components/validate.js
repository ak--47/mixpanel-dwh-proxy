/**
 * @fileoverview validates incoming environment variables
 */


/** @typedef {import('../types').Targets} Targets */
/** @typedef {import('../types').EnvVars} Vars */

// each destination declares its own requiredEnvVars (+ optional validateEnv
// for conditional rules); see middleware/index.js + types.d.ts → Destination
const registry = require('../middleware');

const defaultEventsTableName = 'events';
const defaultUsersTableName = 'users';
const defaultGroupsTableName = 'groups';
const defaultWarehouse = 'MIXPANEL';

/**
 * @param  {Vars} PARAMS={...process.env}
 */
// @ts-ignore
function validate(PARAMS = { ...process.env }) {

	for (const key in PARAMS) {
		if (PARAMS[key] === undefined) delete PARAMS[key];
		else if (PARAMS[key] === '') delete PARAMS[key];
		else if (PARAMS[key] === null) delete PARAMS[key];
		else {
			// case insensitive
			PARAMS[key?.toLowerCase()] = PARAMS[key];
			PARAMS[key?.toUpperCase()] = PARAMS[key];
			PARAMS[key] = PARAMS[key];
		}
	}
	const errors = [];
	let { DESTINATIONS = "",
		EVENTS_TABLE_NAME,
		USERS_TABLE_NAME,
		GROUPS_TABLE_NAME,
		QUEUE_MAX = 0,
		QUEUE_INTERVAL = 600,
	} = PARAMS;

	/** @type {Targets[]} */
	const TARGETS = DESTINATIONS.split(',').map(wh => wh.trim().toUpperCase()).filter(a => a);
	// @ts-ignore
	PARAMS.TARGETS = TARGETS;

	if (!EVENTS_TABLE_NAME) {
		PARAMS.EVENTS_TABLE_NAME = defaultEventsTableName;
		EVENTS_TABLE_NAME = defaultEventsTableName;
		process.env.EVENTS_TABLE_NAME = defaultEventsTableName;
	}
	if (!USERS_TABLE_NAME) {
		PARAMS.USERS_TABLE_NAME = defaultUsersTableName;
		USERS_TABLE_NAME = defaultUsersTableName;
		process.env.USERS_TABLE_NAME = defaultUsersTableName;
	}
	if (!GROUPS_TABLE_NAME) {
		PARAMS.GROUPS_TABLE_NAME = defaultGroupsTableName;
		GROUPS_TABLE_NAME = defaultGroupsTableName;
		process.env.GROUPS_TABLE_NAME = defaultGroupsTableName;
	}

	if (TARGETS.length === 0) {
		TARGETS.push(defaultWarehouse);
	}

	// registry-driven validation: every listed destination must exist in the
	// registry and have all of its requiredEnvVars set; destinations with
	// conditional rules (snowflake, azure) also expose validateEnv()
	for (const target of TARGETS) {
		const destination = registry[target.toLowerCase()];
		if (!destination) {
			errors.push(new Error(`unknown destination: ${target} (supported: ${Object.keys(registry).join(', ')})`));
			continue;
		}
		for (const envVar of destination.requiredEnvVars) {
			if (!PARAMS[envVar]) errors.push(new Error(`${envVar} is required`));
		}
		if (destination.validateEnv) {
			for (const message of destination.validateEnv(PARAMS)) {
				errors.push(new Error(message));
			}
		}
	}

	if (errors.length) {
		// aggregate every problem into one error so a misconfigured boot
		// reports the full fix, not just the first missing var
		const message = errors.map(error => error.message).join('\n');
		console.error(message);
		throw new Error(message);
	}

	// now set env vars for each param, case insensitive
	for (const key in PARAMS) {
		process.env[key?.toLowerCase()] = String(PARAMS[key]);
		process.env[key?.toUpperCase()] = String(PARAMS[key]);
		process.env[key] = String(PARAMS[key]);
	}

	return PARAMS;
};


module.exports = validate;