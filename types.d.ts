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
export type Destinations = "REDSHIFT" | "BIGQUERY" | "SNOWFLAKE" | "MIXPANEL" | string;

export type Endpoints = "track" | "engage" | "groups";
export type Entities = "event" | "user" | "group";
export type AllEntities = Endpoints & Entities;

export type IncomingData = MixpanelEvent[] | UserUpdate[] | GroupUpdate[];
export type WarehouseData = FlatEvent[] | FlatUserUpdate[] | FlatGroupUpdate[];

export type MixpanelEvent = {
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

export type FlatEvent = {
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

type profileOperations = "$set" | "$set_once" | "$append" | "$delete" | "$union";

export type UserUpdate = {
  //will schematize
  $token: string;
  $distinct_id: string;
  $ip?: string;
  //things we will not schematize and should be type JSON
  [key in profileOperations]?: {
    [key: string]: string; // Key-value pairs for group properties
  };
};

export type FlatUserUpdate = {
  //will schematize
  $token: string;
  $distinct_id: string;
  $ip?: string;
  operation: profileOperations;
  //things we will not schematize and should be type JSON
  [key: string]: string;
};

export type GroupUpdate = {
  //will schematize
  $token: string;
  $group_key: string;
  $group_id: string;
  //things we will not schematize and should be type JSON
  [key in profileOperations]?: {
    [key: string]: string;
  };
};

export type FlatGroupUpdate = {
  //will schematize
  $token: string;
  $group_key: string;
  $group_id: string;
  operation: profileOperations;
  //things we will not schematize and should be type JSON
  [key: string]: string;
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
export interface SchemaField {
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
  dest: Destinations; // Destination of the insert operation
  status: "success" | "error" | string; // Status of the insert operation
  insertedRows?: number; // Number of rows successfully inserted
  failedRows?: number; // Number of rows that failed to insert
  duration?: number; // Duration of the insert operation in milliseconds
  errors?: any[]; // Any errors encountered during the operation
  errorMessage?: string; // Error message if the operation failed
  meta?: any; // Additional metadata
};

export type logEntry = StringOnlyTuple | StringObjectTuple;
type StringOnlyTuple = [string];
type StringObjectTuple = [string, object];
