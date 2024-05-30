
const isDebugMode = process.env.NODE_OPTIONS?.includes('--inspect') || process.env.NODE_OPTIONS?.includes('--inspect-brk');
if (isDebugMode) {
	console.log('JEST IS VERBOSELY LOGGING');
}
else {
	console.log('JEST IS NOT VERBOSELY LOGGING');

}

module.exports = {
	verbose: isDebugMode,

};
