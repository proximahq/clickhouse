import type {Pool} from 'undici';

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

export interface BatchParams {
  q: BatchTable;
  queryId?: string;
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
  insertBatch: (q: BatchTable, queryId?: string) => Promise<any>;
  ping: () => Promise<any>;
}
