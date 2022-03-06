import type {Pool, Dispatcher} from 'undici';
import {StreamData} from 'undici/types/dispatcher';
type QueryStringParams = object | any[];

export interface ClickhouseOptions {
  host: string;
  port?: number;
  db?: string;
  protocol: string;
  user?: string;
  password?: string;
  connections?: Pool.Options['connections'];
}

export interface Query {
  query?: string;
  params?: QueryStringParams;
}

export interface QueryStreamParams {
  query: string;
  params?: QueryStringParams;
  opaque?: (...args: any[]) => void;
  factory?: Dispatcher.StreamFactory;
}

export interface BatchParams extends Query {
  table: string;
  items: any[];
}

export interface StreamQueryHandler extends QueryStreamParams {
  path?: string;
  method?: 'GET' | 'POST';
}

export interface QueryHandler extends Query {
  path?: string;
  method?: 'GET' | 'POST';
  onSuccess: (data: any) => void;
  onError: (data: any) => void;
  factory?: Dispatcher.StreamFactory;
  opaque?: any;
}

export interface ClickHouseData {
  [key: string]: number | string | null;
}

export interface ClickhouseQueryResults {
  status: 'ok';
  type: 'json' | 'plain';
  statistics?: {elapsed: number; rows_read: number; bytes_read: number};
  data?: ClickHouseData[];
  txt?: string;
  rows: number;
  meta?: [{name: string; type: string}];
}

export interface ClickhouseQueryError {
  error: Error;
  status: 'error';
  statusCode?: number;
}

export interface ClickHousePool {
  query: (...Query) => Promise<Promise<ClickhouseQueryResults>>;
  queryStream: (s: QueryStreamParams) => Promise<StreamData>;
  selectJson: (...Query) => Promise<ClickhouseQueryResults>;
  insertBatch: (BatchParams) => Promise<ClickhouseQueryResults>;
  ping: (path?: string) => Promise<Promise<ClickhouseQueryResults>>;
  close: () => void;
}
