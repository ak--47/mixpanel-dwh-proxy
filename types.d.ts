export type Runtimes =
  | "AWS"
  | "LAMBDA"
  | "GCP"
  | "CLOUD_FUNCTIONS"
  | "CLOUD_RUN"
  | "AZURE_FUNCTIONS"
  | "FUNCTIONS"
  | "AZURE"
  | "LOCAL"
  | string;

export type Warehouse = "REDSHIFT" | "BIGQUERY" | "SNOWFLAKE" | "MIXPANEL";
export type Lake = "S3" | "GCS" | "AZURE_BLOB";
export type Targets = Warehouse | Lake | string;

export type Endpoints = "track" | "engage" | "groups";
export type Entities = "event" | "user" | "group";
export type AllEntities = Endpoints & Entities;

export type IncomingData = MixpanelEvent[] | UserUpdate[] | GroupUpdate[];
export type FlatData = FlatEvent[] | FlatUserUpdate[] | FlatGroupUpdate[];
export type SchematizedData = SchematizedEvent[] | SchematizedUserUpdate[] | SchematizedGroupUpdate[];

type MixpanelEvent = {
  event: string;
  properties: {
    //things we will schematize
    time: number;
    token: string;
    $device_id: string;
    $insert_id: string;

    //nullables
    $user_id?: string;
    distinct_id?: string;

    //things we will not schematize and should be type JSON
    [key: string]: string | number | boolean | object | any[];
  };
};

type FlatEvent = {
  event: string;
  //things we will schematize
  time: number;
  token: string;
  $device_id: string;
  $insert_id: string;

  //nullables
  $user_id?: string;
  distinct_id?: string;

  //things we will not schematize and should be type JSON
  [key: string]: string | number | boolean | object | any[];
};

type SchematizedEvent = FlatEvent & {
  properties: {
    [key: string]: string | number | boolean | object | any[];
  };
};

type profileOperations = "$set" | "$set_once" | "$append" | "$delete" | "$union";

type UserUpdate = {
  //will schematize
  $token: string;
  $distinct_id: string;
  $ip?: string;
  //things we will not schematize and should be type JSON
  [key in profileOperations]?: {
    [key: string]: string; // Key-value pairs for group properties
  };
};

type FlatUserUpdate = {
  //will schematize
  $token: string;
  $distinct_id: string;
  $ip?: string;
  operation: profileOperations;
  //things we will not schematize and should be type JSON
  [key: string]: string;
};

type SchematizedUserUpdate = FlatUserUpdate & {
  properties: {
    [key: string]: string | number | boolean | object | any[];
  };
};

type GroupUpdate = {
  //will schematize
  $token: string;
  $group_key: string;
  $group_id: string;
  //things we will not schematize and should be type JSON
  [key in profileOperations]?: {
    [key: string]: string;
  };
};

type FlatGroupUpdate = {
  //will schematize
  $token: string;
  $group_key: string;
  $group_id: string;
  operation: profileOperations;
  //things we will not schematize and should be type JSON
  [key: string]: string;
};

type SchematizedGroupUpdate = FlatGroupUpdate & {
  properties: {
    [key: string]: string | number | boolean | object | any[];
  };
};

// TypeScript types for inferred schema
type JSONType = "ARRAY" | "OBJECT" | "JSON";
type NumberType = "FLOAT" | "INT";
type DateType = "DATE" | "TIMESTAMP";
type SpecialType = "PRIMARY_KEY" | "FOREIGN_KEY" | "LOOKUP_KEY";
type BasicType = "STRING" | "BOOLEAN" | JSONType | NumberType | DateType | SpecialType;

// Vendor-specific types
type BigQueryTypes =
  | "TIMESTAMP"
  | "INT64"
  | "FLOAT64"
  | "DATE"
  | "TIMESTAMP"
  | "BOOLEAN"
  | "STRING"
  | "ARRAY"
  | "STRUCT"
  | "JSON"
  | "RECORD";
type SnowflakeTypes = "VARIANT" | "STRING" | "BOOLEAN" | "NUMBER" | "FLOAT" | "TIMESTAMP" | "DATE";
type RedshiftTypes = "SUPER" | "VARCHAR" | "BOOLEAN" | "INTEGER" | "REAL" | "TIMESTAMP" | "DATE";

// Schema field interface
interface SchemaField {
  name: string;
  type: BasicType | BigQueryTypes | SnowflakeTypes | RedshiftTypes;
}

export type TableNames = {
  eventTable: string;
  userTable: string;
  groupTable: string;
};
// Schema type
export type Schema = SchemaField[];

// Insert result type
export type InsertResult = {
  status: "success" | "error" | string; // Status of the insert operation
  insertedRows?: number; // Number of rows successfully inserted
  failedRows?: number; // Number of rows that failed to insert
  duration?: number; // Duration of the insert operation in milliseconds
  errors?: any[]; // Any errors encountered during the operation
  errorMessage?: string; // Error message if the operation failed
  meta?: any; // Additional metadata
  message?: string; // Message from the operation
};

export type MiddlewareResponse = {
  name: Targets;
  result: InsertResult;
};

export type logEntry = StringOnlyTuple | StringObjectTuple;
type StringOnlyTuple = [string];
type StringObjectTuple = [string, object];

// TYPES FOR MIDDLEWARE
export type BigQueryVars = {
  bigquery_project: string;
  bigquery_dataset: string;
  bigquery_table: string;
  bigquery_keyfile: string;
  bigquery_service_account_email: string;
  bigquery_service_account_private_key: string;
};

export type SnowflakeVars = {
  snowflake_account: string;
  snowflake_user: string;
  snowflake_password: string;
  snowflake_database: string;
  snowflake_schema: string;
  snowflake_warehouse: string;
  snowflake_role: string;
  snowflake_access_url: string;
  snowflake_stage?: string;
  snowflake_pipe?: string;
  snowflake_private_key?: string;
  snowflake_region?: string;
  snowflake_provider?: string;
};

export type RedshiftVars = {
  redshift_workgroup: string;
  redshift_database: string;
  redshift_region: string;
  redshift_access_key_id: string;
  redshift_secret_access_key: string;
  redshift_schema_name: string;
  redshift_session_token?: string;
};

export type GCSVars = {
  gcs_project: string;
  gcs_bucket: string;
  gcs_service_account: string;
  gcs_service_account_private_key: string;
  gcs_keyfile: string;
};

export type S3Vars = {
  s3_bucket: string;
  s3_region: string;
  s3_access_key_id: string;
  s3_secret_access_key: string;
};

export type AzureVars = {
  azure_account: string;
  azure_key: string;
  azure_container: string;
  azure_connection_string?: string;
};

export type MixpanelVars = {
	mixpanel_token?: string;
	mixpanel_region?: string;
}

export type otherVars = {
  DESTINATION: String;
  EVENTS_TABLE_NAME: string;
  USERS_TABLE_NAME: string;
  GROUPS_TABLE_NAME: string;
  BATCH_SIZE: number;
  MAX_RETRIES: number;
};

export type EnvVars = BigQueryVars & SnowflakeVars & RedshiftVars & GCSVars & otherVars;
