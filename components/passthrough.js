/**
 * @fileoverview always-on passthrough routes to Mixpanel, regardless of DESTINATIONS:
 * - /lib.min.js + /lib.js → CDN (so MIXPANEL_CUSTOM_LIB_URL can point here)
 * - /record → session replay
 * - /flags → feature flags
 * these MUST be registered before the body parsers: the proxy streams the
 * raw request through and a consumed body would break the upstream call
 */

const { createProxyMiddleware } = require('http-proxy-middleware');
const NODE_ENV = process.env.NODE_ENV || 'prod';
const REGION = process.env.REGION || 'US';
const BASE_URL = `https://api${REGION?.toUpperCase() === "EU" ? '-eu' : ''}.mixpanel.com`;

module.exports = function (app, environment = NODE_ENV, URL = BASE_URL) {

	app.use('/lib.min.js', createProxyMiddleware({
		target: 'https://cdn.mxpnl.com/libs/mixpanel-2-latest.min.js',
		changeOrigin: true,
		pathRewrite: { '^/lib.min.js': '' },
		logLevel: environment === "prod" ? "error" : "silent"
	}));

	app.use('/lib.js', createProxyMiddleware({
		target: 'https://cdn.mxpnl.com/libs/mixpanel-2-latest.js',
		changeOrigin: true,
		pathRewrite: { '^/lib.js': '' },
		logLevel: environment === "prod" ? "error" : "silent"
	}));

	app.use('/record', createProxyMiddleware({
		target: URL,
		changeOrigin: true,
		pathRewrite: { '^/record': '/record' },
		logLevel: environment === "prod" ? "error" : "silent",
	}));

	app.use('/flags', createProxyMiddleware({
		target: URL,
		changeOrigin: true,
		pathRewrite: { '^/flags': '/flags' },
		logLevel: environment === "prod" ? "error" : "silent",
	}));
};
