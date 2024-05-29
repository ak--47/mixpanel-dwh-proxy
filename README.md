# Mixpanel → DWH Proxy

Mixpanel DWH Proxy sits in front of your client-side mixpanel code and mixpanel's servers. In addition to sending analytics data to Mixpanel, it will also create and append the same analytics data to your data warehouse. 

It is intended to be deployed in serverless environments like Google Cloud Run, AWS Lambda, and Azure Functions. 

This proxy allows you to modify each record before it gets sent to Mixpanel or other destinations, providing a flexible solution for data processing, and an out-of-the-box (free) way to bring your Mixpanel data into your datawarehouse.

## Features
- Supports multiple data warehouse destinations:
	- BigQuery
	- Snowflake
	- Redshift
	- Mixpanel

- Proxies requests to Mixpanel's `/track`, `/engage`, and `/groups` endpoints to tables in your data warehouse.

- Schematizes standard record fields.

- Can run on cheap on-demand serverless compute platforms.


## Deployment
To deploy the Mixpanel DWH Proxy, you need to set the following environment variables (case insensitive):

### Required Environment Variables

- `WAREHOUSES`: Comma-separated list of destinations (`MIXPANEL`, `BIGQUERY`, `SNOWFLAKE`, `REDSHIFT`).

### Optional Environment Variables
- `PORT`: The port the server will listen on (default: 8080).
- `FRONTEND_URL`: The URL of your frontend application (for CORS). Set to `none` to disable CORS.
- `RUNTIME`: The runtime environment (`LOCAL`, `GCP`, `AWS`, `AZURE`).
- `EVENTS_TABLE_NAME`: The name of the events table. (default `events`)
- `USERS_TABLE_NAME`: The name of the users table. (default `users`)
- `GROUPS_TABLE_NAME`: The name of the groups table. (default `groups`)

### BigQuery Specific Variables
- `bigquery_project`: Your BigQuery project ID.
- `bigquery_dataset`: Your BigQuery dataset ID.
- `bigquery_service_account`: Your BigQuery service account email.
- `bigquery_service_account_pass`: Your BigQuery service account private key.
- `bigquery_keyfile`: Path to your BigQuery keyfile.

### Snowflake Specific Variables
- `snowflake_account`: Your Snowflake account name.
- `snowflake_user`: Your Snowflake username.
- `snowflake_password`: Your Snowflake password.
- `snowflake_database`: Your Snowflake database name.
- `snowflake_schema`: Your Snowflake schema name.
- `snowflake_warehouse`: Your Snowflake warehouse name.
- `snowflake_role`: Your Snowflake role.
- `snowflake_access_url`: Your Snowflake access URL.
- `snowflake_stage`: Your Snowflake stage (optional).
- `snowflake_pipe`: Your Snowflake pipe (optional).

### Redshift Specific Variables
- `redshift_workgroup`: Your Redshift workgroup.
- `redshift_database`: Your Redshift database name.
- `redshift_access_key_id`: Your Redshift access key ID.
- `redshift_secret_access_key`: Your Redshift secret access key.
- `redshift_session_token`: Your Redshift session token (optional).
- `redshift_region`: Your Redshift region.
- `redshift_schema_name`: Your Redshift schema name.

### Mixpanel Specific Variables
- `mixpanel_token`: Your Mixpanel project token (optional).

## Usage
Once deployed, you can use the following client-side configuration to initialize Mixpanel and send data through the proxy:

```javascript
const PROXY_URL = `http://localhost:8080`; // the URL of your deployed proxy
const MIXPANEL_CUSTOM_LIB_URL = `${PROXY_URL}/lib.min.js`;
// Mixpanel snippet
(function (f, b) { ... })(document, window.mixpanel || []);

// You may pass an empty string as the token to init() 
mixpanel.init("", { api_host: PROXY_URL });

// This event will get sent to your Mixpanel project via the proxy
mixpanel.track("test", { "foo": "bar" });
