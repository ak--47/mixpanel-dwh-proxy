/**
 * @fileoverview the destination registry: every supported destination, keyed by
 * the (lowercase) name users put in the DESTINATIONS env var.
 *
 * requires must stay static — the lambda build (esbuild) cannot bundle dynamic requires.
 * to add a destination: create the module with the typed export tail (see pubsub.js,
 * the reference implementation), then add one line here.
 */

/** @type {import('../types').DestinationRegistry} */
module.exports = {
	mixpanel: require('./mixpanel'),
	bigquery: require('./bigquery'),
	pubsub: require('./pubsub'),
	snowflake: require('./snowflake'),
	redshift: require('./redshift'),
	gcs: require('./gcs'),
	s3: require('./s3'),
	azure: require('./azure'),
};
