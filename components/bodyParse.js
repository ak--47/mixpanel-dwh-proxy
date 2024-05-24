const bodyParser = require('body-parser');

module.exports = function (app) {
	app.use(bodyParser.json());
	app.use(bodyParser.urlencoded({ extended: true }));
	app.use(bodyParser.text({ type: 'text/plain' }));

	// CATCHING ERRORS
	app.use((err, req, res, next) => {
		console.error(err.stack);
		res.status(500).send(`Something went wrong!\n\n${err?.message || err}`);
	});


};