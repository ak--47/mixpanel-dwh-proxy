// @ts-nocheck
/**
 * verifies every destination in the registry conforms to the Destination contract
 * (see types.d.ts → Destination) and that envSample documents every required var
 */
const { readFileSync } = require('fs');
const path = require('path');

const registry = require('../middleware');

const CANONICAL_DESTINATIONS = ['mixpanel', 'bigquery', 'pubsub', 'snowflake', 'redshift', 'gcs', 's3', 'azure'];

describe('DESTINATION CONTRACT', () => {
	test('registry contains exactly the canonical destinations', () => {
		expect(Object.keys(registry).sort()).toEqual([...CANONICAL_DESTINATIONS].sort());
	});

	for (const name of CANONICAL_DESTINATIONS) {
		describe(name, () => {
			test('is a callable send function', () => {
				expect(typeof registry[name]).toBe('function');
			});

			test('has init and drop lifecycle methods', () => {
				expect(typeof registry[name].init).toBe('function');
				expect(typeof registry[name].drop).toBe('function');
			});

			test('declares requiredEnvVars as lowercase string array', () => {
				const { requiredEnvVars } = registry[name];
				expect(Array.isArray(requiredEnvVars)).toBe(true);
				for (const envVar of requiredEnvVars) {
					expect(typeof envVar).toBe('string');
					expect(envVar).toBe(envVar.toLowerCase());
				}
			});

			test('validateEnv (if present) is a function', () => {
				const { validateEnv } = registry[name];
				if (validateEnv !== undefined) expect(typeof validateEnv).toBe('function');
			});
		});
	}

	test('every required env var is documented in envSample', () => {
		const envSample = readFileSync(path.join(__dirname, '..', 'envSample'), 'utf-8');
		const missing = [];
		for (const name of CANONICAL_DESTINATIONS) {
			for (const envVar of registry[name].requiredEnvVars) {
				if (!envSample.toLowerCase().includes(envVar)) missing.push(`${name}: ${envVar}`);
			}
		}
		expect(missing).toEqual([]);
	});
});
