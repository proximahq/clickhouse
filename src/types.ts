import Stream from 'stream';
import type {Pool} from 'undici';
import {supportedJSONFormats, supportedRawFormats} from './constants';

export type JSONDataFormat = typeof supportedJSONFormats[number];
export type RawDataFormat = typeof supportedRawFormats[number];
export type DataFormat = JSONDataFormat | RawDataFormat;

export type JsonValue =
  | string
  | number
  | boolean
  | JsonObject
  | JsonArray
  | null;
export interface JsonArray extends Array<JsonValue> {}
export type JsonObject = {[Key in string]?: JsonValue};

type QueryStringParams = object | any[];
export interface ClickhouseOptions {
  // Options Clickhouse
  host: string;
  port?: number;
  db?: string;
  protocol: string;
  user?: string;
  password?: string;

  size?: number;
  sessionTimeout?: number;
  compression?: boolean;
  // Options for Pool
  connections?: Pool.Options['connections'];
  keepAliveMaxTimeout?: Pool.Options['keepAliveMaxTimeout'];
  headersTimeout?: Pool.Options['headersTimeout'];
  bodyTimeout?: Pool.Options['bodyTimeout'];
}

export interface QueryParams {
  query?: string;
  params?: QueryStringParams;
  queryId?: string;
}

export interface ObjInterface {
  session_timeout?: number;
  output_format_json_quote_64bit_integers?: number;
  enable_http_compression?: number;
  query_id?: string;
  session_id?: string;
  query?: string;
}
export interface BatchTable {
  table: string;
  items: any[];
}

export interface StreamInsertParams {
  table: string;
  items: Stream.Readable;
  format?: DataFormat;
}

export interface StreamInsertResult {
  query_id: string;
}

export interface Client {
  open: () => void;
  close: () => void;
  query: (
    queryString: string,
    params?: any[],
    queryId?: string,
  ) => Promise<any>;
  selectJson: (
    queryString: string,
    params?: any[],
    queryId?: string,
  ) => Promise<any>;
  insertStream: (q: StreamInsertParams) => Promise<StreamInsertResult>;

  insertBatch: (
    q: BatchTable,
    fallback?: (q: BatchTable) => Promise<any>,
  ) => Promise<any>;
  ping: () => Promise<any>;
}
