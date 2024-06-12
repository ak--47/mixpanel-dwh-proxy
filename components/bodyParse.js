/**
 * @fileoverview body parser middleware for express
 */

const bodyParser = require('body-parser');

module.exports = function (app) {
	app.use(bodyParser.json({limit: '50mb'}));
	app.use(bodyParser.urlencoded({ extended: true, limit: '50mb'}));
	app.use(bodyParser.text({ type: 'text/plain', limit: '50mb'}));

	// CATCHING ERRORS
	app.use((err, req, res, next) => {
		console.error(err.stack);
		res.status(500).send(`Something went wrong!\n\n${err?.message || err}`);
	});


};