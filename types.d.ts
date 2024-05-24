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
export type Destinations =
  | "REDSHIFT"
  | "BIGQUERY"
  | "SNOWFLAKE"
  | "MIXPANEL"
  | string;

// TypeScript types for inferred schema
type JSONType = "ARRAY" | "OBJECT" | "JSON";
type NumberType = "FLOAT" | "INT";
type DateType = "DATE" | "TIMESTAMP";
type SpecialType = "PRIMARY_KEY" | "FOREIGN_KEY" | "LOOKUP_KEY";
type BasicType =
  | "STRING"
  | "BOOLEAN"
  | JSONType
  | NumberType
  | DateType
  | SpecialType;

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
type SnowflakeTypes =
  | "VARIANT"
  | "STRING"
  | "BOOLEAN"
  | "NUMBER"
  | "FLOAT"
  | "TIMESTAMP"
  | "DATE";
type RedshiftTypes =
  | "SUPER"
  | "VARCHAR"
  | "BOOLEAN"
  | "INTEGER"
  | "REAL"
  | "TIMESTAMP"
  | "DATE";

// Schema field interface
export interface SchemaField {
  name: string;
  type: BasicType | BigQueryTypes | SnowflakeTypes | RedshiftTypes;
}

// Schema type
export type Schema = SchemaField[];

// CSV record and batch types
export type dataRecord = {
  [key: string]: string;
};

export type dataBatch = dataRecords[];

// Insert result type
export type InsertResult = {
  status: "success" | "error"; // Status of the insert operation
  insertedRows?: number; // Number of rows successfully inserted
  failedRows?: number; // Number of rows that failed to insert
  duration: number; // Duration of the insert operation in milliseconds
  errors?: any[]; // Any errors encountered during the operation
  errorMessage?: string; // Error message if the operation failed
  meta?: any; // Additional metadata
};
