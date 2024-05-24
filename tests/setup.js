const { spawn } = require('child_process');
const timeout = 100000;

// This function is called once before running the tests to start the test server.
module.exports = async () => {
	console.log('starting test server + frontend...\n\n');
	global.__SERVER__ = spawn('npm', ['run', 'dev']);
	global.__FRONTEND__ = spawn('npm', ['run', 'frontend']);

	const mainServerStarted = new Promise(resolve => {
		global.__SERVER__.stdout.once('data', data => {
			console.log('Server started');
			resolve('started');

		});
	});

	const frontEndStarted = new Promise(resolve => {
		global.__FRONTEND__.stdout.once('data', data => {
			console.log('Frontend started');
			resolve('started');
		});
	});

	
	const timeOutMain = new Promise(resolve => {
		setTimeout(() => {
			throw new Error('main proxy timeout');
			// resolve('timeout');
		}, timeout);
	});

	
	const timeOutFE = new Promise(resolve => {
		setTimeout(() => {
			throw new Error('front end timeout');
			// resolve('timeout');
		}, timeout);
	});

	const started = await Promise.all([mainServerStarted, frontEndStarted]);
	await sleep(2000);
	console.log('starting tests...\n\n');



	
};



function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}