/**
 * @fileoverview ensures that lib calls proxy through to the CDN
 * also provides forwarding for session replay via /record
 */

const { createProxyMiddleware } = require('http-proxy-middleware');
const NODE_ENV = process.env.NODE_ENV || 'prod';
const REGION = process.env.REGION || 'US';
const BASE_URL = `https://api${REGION?.toUpperCase() === "EU" ? '-eu' : ''}.mixpanel.com`;
if (!BASE_URL) throw new Error('BASE_URL is required; mixpanel middleware is not ready');

module.exports = function (app, environment = NODE_ENV, URL = BASE_URL) {

	app.use('/lib.min.js', createProxyMiddleware({
		target: 'https://cdn.mxpnl.com/libs/mixpanel-2-latest.min.js',
		changeOrigin: true,
		pathRewrite: { '^/lib.min.js': '' },
		logLevel: environment === "prod" ? "error" : "debug"
	}));

	app.use('/lib.js', createProxyMiddleware({
		target: 'https://cdn.mxpnl.com/libs/mixpanel-2-latest.js',
		changeOrigin: true,
		pathRewrite: { '^/lib.js': '' },
		logLevel: environment === "prod" ? "error" : "debug"
	}));

	app.use('/record', createProxyMiddleware({
		target: URL,
		changeOrigin: true,
		pathRewrite: { '^/record': '/record' },
		logLevel: environment === "prod" ? "error" : "debug",
	}));
};