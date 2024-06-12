
const log = require('../components/logger.js');

const isDebugMode = process.env.NODE_OPTIONS?.includes('--inspect') || process.env.NODE_OPTIONS?.includes('--inspect-brk');
if (isDebugMode) {
	log.verbose(true);
	log.cli(true);
	log('JEST IS VERBOSELY LOGGING');

}
else {
	log.verbose(true);
	log.cli(true);
	console.log('JEST IS NOT VERBOSELY LOGGING');
	log.verbose(false);
	log.cli(false);
}

/** @type {import('jest').Config} */
const jestConfig = {
	verbose: isDebugMode,
	watch: false,
	

};

module.exports = jestConfig;
