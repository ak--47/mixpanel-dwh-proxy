module.exports = function (app, FRONTEND_URL = "") {
	// CORS Middleware
	app.use((req, res, next) => {
		res.header('Access-Control-Allow-Origin', FRONTEND_URL || req.headers.origin || "*");
		res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
		res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
		res.header('Access-Control-Allow-Credentials', 'true');
		if (!FRONTEND_URL) {
			// Most permissive CSP headers (unsafe)
			res.header('Content-Security-Policy', "default-src 'none'; script-src 'unsafe-inline' 'unsafe-eval' *; connect-src *; img-src * data:; style-src 'unsafe-inline' *;");
		}
		next();
	});

	// CORS Options
	app.options('*', (req, res) => {
		res.header('Access-Control-Allow-Origin',  FRONTEND_URL || req.headers.origin || "*");
		res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
		res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
		res.header('Access-Control-Allow-Credentials', 'true');
		if (!FRONTEND_URL) {
			// Most permissive CSP headers (unsafe)
			res.header('Content-Security-Policy', "default-src 'none'; script-src 'unsafe-inline' 'unsafe-eval' *; connect-src *; img-src * data:; style-src 'unsafe-inline' *;");
		}
		res.sendStatus(200);
	});

};
