import {Writable} from 'stream';
import {format} from 'sqlstring';
import dbg from 'debug';
import {getErrorObj} from './error';
import {
  QueryHandler,
  StreamQueryHandler,
  ClickhouseOptions,
  ClickhouseQueryError,
  ClickHousePool,
} from './types';
import {
  JSON_EACH_SUFFIX,
  JSON_SUFFIX,
  fn,
  defaultOpts,
  cleanup,
  getUndici,
  bumpHeaders,
  grabInstance,
} from './utils';

const log = dbg('proxima:clickhouse-driver');

const getStreamHandler = ({
  host,
  port,
  protocol,
  db: database,
  user,
  password,
  connections,
}: ClickhouseOptions) => {
  const pool = getUndici({host, port, protocol, connections});
  const headers = bumpHeaders({user, password});
  return ({
    query,
    path,
    opaque,
    method = 'POST',
    factory,
  }: StreamQueryHandler) => {
    log('streaming for query %s', query);
    log('streaming headers %o', headers);
    log('streaming opaque found', !!opaque);
    log('streaming factory found', !!factory);
    log('streaming method', method);

    const partials: string[] = [];
    const fctr = ({opaque, statusCode}) => {
      const w = new Writable({
        defaultEncoding: 'utf-8',
        write(partial: string, _, callback) {
          (opaque as string[]).push(partial);
          callback();
        },
      });
      w.on('error', () => {
        throw new Error('stream error');
      });
      if (statusCode !== 200) {
        w.emit('error', 'err');
      }
      return w;
    };

    const p = path ?? '/';
    return pool.stream(
      {
        path: p,
        method,
        headers: {
          ...(database && {'X-ClickHouse-Database': database}),
          ...headers,
        },
        body: query,
        opaque: opaque ? opaque : partials,
      },
      factory ? factory : fctr,
    );
  };
};

const getHandler = ({
  host,
  port,
  protocol,
  db: database,
  user,
  password,
  connections,
}: ClickhouseOptions) => {
  const Pool = getUndici({host, port, protocol, connections});
  const headers = bumpHeaders({user, password});

  return async ({
    query,
    path,
    method = 'POST',
    onSuccess = fn,
    onError = fn,
  }: QueryHandler) => {
    const p = path ?? '/';

    const {body, statusCode} = await Pool.request({
      path: p,
      method,
      body: query,
      headers: {
        ...(database && {'X-ClickHouse-Database': database}),
        ...headers,
      },
    });

    const txt = await body.text();

    if (statusCode === 200) {
      try {
        const results = JSON.parse(txt);
        log('success');
        onSuccess({...results, status: 'ok', type: 'json'});
      } catch (ignore) {
        log('success without format');
        onSuccess({status: 'ok', type: 'plain', txt: txt});
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

const clickhouse = (opts: ClickhouseOptions): ClickHousePool => {
  log('init');
  const options = {...defaultOpts, ...(opts || {})};
  const exec = getHandler(options);
  const execStream = getStreamHandler(options);

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
    queryStream: ({query, params, opaque, factory}) => {
      const q = cleanup(query ?? '');
      const executableQuery = `${format(q, params)};`;
      return execStream({
        query: executableQuery,
        opaque,
        factory,
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
          onSuccess: data => {
            log('insertBatch success');
            res(data);
          },
        });
      });
    },

    ping: p => {
      log('ping');
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
    close: () => {
      log('closing');
      const instance = grabInstance();
      instance && !instance.closed && instance.close();
    },
  };
};

export * from './types';
export default clickhouse;
