import {Client} from 'undici';
import {format} from 'sqlstring';
import dbg from 'debug';
import {getErrorObj} from './error';

const log = dbg('clickhouse');

// Constants
const TRAILING_SEMI = /;+$/;
const JSON_SUFFIX = 'FORMAT JSON;';
const JSON_EACH_SUFFIX = 'FORMAT JSONEachRow';

// Utils
// @ts-ignore
const fn = (...args: any[]): void => {};

const defaultOpts = {
  host: 'localhost',
  port: 8123,
  db: 'default',
  protocol: 'http',
  user: '',
  password: '',
};

const cleanup = (str: string) => str.replace(TRAILING_SEMI, '');

let undiciClient: Client;

const getUndici = (host: string, port: number, protocol: string): Client => {
  if (undiciClient) return undiciClient;
  const u = `${protocol}://${host}:${port}`;
  undiciClient = new Client(u);
  return undiciClient;
};

type QueryStringParams = object | any[];
export interface ClickhouseOptions {
  host: string;
  port?: number;
  db: string;
  protocol: string;
  user?: string;
  password?: string;
}

export interface Query {
  query?: string;
  params?: QueryStringParams;
}

export interface BatchParams extends Query {
  table: string;
  items: any[];
}

export interface QueryHandler extends Query {
  path?: string;
  db?: string;
  method?: 'GET' | 'POST';
  onSuccess: (data: any) => void;
  onError: (data: any) => void;
}
interface GenericData {
  [key: string]: number | string | null;
}
export interface ClickhouseQueryResults {
  status: 'ok';
  type: 'json' | 'plain';
  statistics?: {elapsed: number; rows_read: number; bytes_read: number};
  data?: GenericData[] | string;
}

export interface ClickhouseQueryError {
  error: Error;
  status: 'error';
  statusCode?: number;
}

export interface ClickHouseClient {
  query: (...Query) => Promise<Promise<ClickhouseQueryResults>>;
  selectJson: (...Query) => Promise<ClickhouseQueryResults>;
  insertBatch: (BatchParams) => Promise<ClickhouseQueryResults>;
  ping: (path?: string) => Promise<Promise<ClickhouseQueryResults>>;
}

const getHandler = ({
  host,
  port,
  protocol,
  user,
  password,
}: ClickhouseOptions) => {
  const client = getUndici(host, port, protocol);
  const headers = {
    'Content-Type': 'application/json',
    ...(user && {'X-ClickHouse-User': user}),
    ...(password && {'X-ClickHouse-Key': password}),
  };

  return async ({
    query,
    path,
    db,
    method = 'POST',
    onSuccess = fn,
    onError = fn,
  }: QueryHandler) => {
    const p = path ?? '/';
    const {body, statusCode} = await client.request({
      path: p,
      method,
      body: query,
      headers: {
        ...(db && {'X-ClickHouse-Database': db}),
        ...headers,
      },
    });

    const txt = await body.text();

    if (statusCode === 200) {
      try {
        const results = JSON.parse(txt);
        onSuccess({...results, status: 'ok', type: 'json'});
      } catch (ignore) {
        onSuccess({status: 'ok', type: 'plain', data: txt});
      }
    } else {
      log(`Error: ${txt}`);
      const e = getErrorObj({statusCode, data: txt});
      const err = {
        statusCode,
        status: 'error',
        error: e,
      } as ClickhouseQueryError;
      onError(err);
    }
  };
};

const clickhouse = (opts: ClickhouseOptions): ClickHouseClient => {
  log('init');
  const options = {...defaultOpts, ...(opts || {})};
  const exec = getHandler(options);
  return {
    query: (query, params = []) => {
      if (!query) {
        throw new Error('query is required');
      }

      const executableQuery = `${format(query, params)};`;
      return new Promise((res, rej) => {
        return exec({
          query: executableQuery,
          onError: rej,
          onSuccess: res,
        });
      });
    },
    selectJson: (query, params = []) => {
      const q = cleanup(query ?? '');
      const executableQuery = `${format(q, params)} ${JSON_SUFFIX};`;

      return new Promise((res, rej) => {
        exec({
          query: executableQuery,
          onError: rej,
          onSuccess: res,
        });
      });
    },
    insertBatch: (q = {}) => {
      const {table, items} = q;
      if (!table) {
        throw new Error('`table` is required for batch insert');
      }
      if (!items) {
        throw new Error('`items` are required for batch insert');
      }
      const path = `/?${new URLSearchParams({
        query: `INSERT INTO ${table} ${JSON_EACH_SUFFIX}`,
      })}`;

      return new Promise((res, rej) => {
        exec({
          query: JSON.stringify(items),
          path,
          onError: rej,
          onSuccess: res,
        });
      });
    },

    ping: p => {
      const path = p ?? `/ping`;
      return new Promise((res, rej) => {
        exec({
          path,
          method: 'GET',
          onError: rej,
          onSuccess: res,
        });
      });
    },
  };
};

export default clickhouse;
