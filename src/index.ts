import {format} from 'sqlstring';
import dbg from 'debug';
import {Connection} from './connection';
import {
  ClickhouseOptions,
  StreamInsertParams,
  BatchTable,
  Client,
} from './types';
import {defaultOpts, JSON_SUFFIX, JSON_EACH_SUFFIX} from './constants';
import {
  cleanupObj,
  cleanup,
  createPathGen,
  genIds,
  encodeValues,
} from './utils';

const log = dbg('proxima:clickhouse-driver:main');
const factoryId = genIds();
const createPath = createPathGen();

export type {Client, ClickhouseOptions};
export const clickhouse = (opts: ClickhouseOptions = defaultOpts): Client => {
  const {
    protocol,
    host,
    port,
    db,
    user,
    password,
    connections,
    keepAliveMaxTimeout,
    headersTimeout,
    bodyTimeout,
  } = opts;

  const client = new Connection({db, user, password});

  const ch = {
    open: () => {
      log('opening connection');

      const p = cleanupObj({
        connections,
        keepAliveMaxTimeout,
        headersTimeout,
        bodyTimeout,
      });
      const u = `${protocol}://${host}:${port}`;
      return client.open(u, p);
    },

    close: () => {
      log('closing connection');
      client && client.isClosed() && client.close();
    },

    query: (queryString: string, params: any[] = [], queryId = factoryId()) => {
      if (!queryString) {
        throw new Error('query is required');
      }
      const executableQuery = format(queryString, params);
      const sessionId = client.getSessionId();
      const path = createPath({
        session_id: sessionId,
        query_id: queryId,
      });
      return client.post(path, executableQuery).finally(() => {
        client.returnSessionId(sessionId as string);
      });
    },

    selectJson: <Results>(
      queryString: string,
      params: any[] = [],
      queryId = factoryId(),
    ) => {
      if (!queryString) {
        throw new Error('query is required');
      }
      const q = cleanup(queryString ?? '');
      const executableQuery = `${format(q, params)} ${JSON_SUFFIX};`;
      const sessionId = client.getSessionId();

      const path = createPath({
        session_id: sessionId,
        query_id: queryId,
      });
      return client.post<Results>(path, executableQuery).finally(() => {
        client.returnSessionId(sessionId as string);
      });
    },

    insertStream: (
      q: StreamInsertParams,
      fallback?: (q: StreamInsertParams, err: any) => Promise<any>,
    ) => {
      const {table, items, format} = q;
      const queryId = factoryId();
      if (!table) {
        throw new Error('`table` is required for batch insert');
      }
      if (!items) {
        throw new Error('`items` are required for batch insert');
      }
      if (!format) {
        throw new Error('`format` is required for batch insert');
      }
      const sessionId = client.getSessionId();
      const path = createPath({
        query: `INSERT INTO ${table} FORMAT ${format}`,
        session_id: sessionId,
        query_id: queryId,
      });

      return client
        .stream(path, {}, encodeValues(items, format))
        .catch(err => {
          if (fallback) {
            return fallback(q, err);
          }
          throw err;
        })
        .finally(() => {
          client.returnSessionId(sessionId as string);
        });
    },

    insertBatch: (
      q: BatchTable,
      fallback?: (q: BatchTable, err: any) => Promise<any>,
    ) => {
      const {table, items} = q;
      const queryId = factoryId();
      if (!table) {
        throw new Error('`table` is required for batch insert');
      }
      if (!items) {
        throw new Error('`items` are required for batch insert');
      }

      const sessionId = client.getSessionId();
      const path = createPath({
        query: `INSERT INTO ${table} ${JSON_EACH_SUFFIX}`,
        session_id: sessionId,
        query_id: queryId,
      });

      const executableQuery = JSON.stringify(items);
      return client
        .post(path, executableQuery)
        .catch(err => {
          if (fallback) {
            return fallback(q, err);
          }
          throw err;
        })
        .finally(() => {
          client.returnSessionId(sessionId as string);
        });
    },
    ping: () => {
      return client.get('/ping');
    },
  };
  return ch;
};
