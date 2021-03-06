import type {Dispatcher, Pool} from 'undici';
import type {URL} from 'url';
import {getErrorObj} from './error';
import {cleanupObj, genIds} from './utils';
import {IncomingHttpHeaders} from 'http';
import dbg from 'debug';
import {OK} from './constants';
import {debug} from 'console';

const log = dbg('proxima:clickhouse-driver:connection');

const factoryId = genIds();

export type Result<R> = {
  statusCode: Dispatcher.ResponseData['statusCode'];
  headers: Dispatcher.ResponseData['headers'];
  data: R;
};

export type DbIfo = {
  db?: string;
  user?: string;
  password?: string;
};

export type DbIfoHeaders = {
  'Accept-Encoding': '*';
  'X-ClickHouse-Database'?: string;
  'X-ClickHouse-User'?: string;
  'X-ClickHouse-Key'?: string;
};

// because undici lazily loads llhttp wasm which bloats the memory
// TODO: hopefully replace with `import` but that causes segfaults
const undici = () => require('undici');

/**
 * Assertion function to make sure that we have a pool
 * @param pool
 */
function assertHasPool<A>(pool: A): asserts pool is NonNullable<A> {
  if (pool === undefined) {
    throw new Error('Connection has not been opened');
  }
}

/**
 * Open an HTTP connection pool
 */
export class Connection {
  private _pool: Pool | undefined;
  private _sessionIds: string[] = [];
  private _hasSessionId: boolean = true;

  private _dbInfoHeaders: Pool | {};

  constructor(dbInfo: DbIfo = {}) {
    this._dbInfoHeaders = cleanupObj({
      'Accept-Encoding': '*',
      'X-ClickHouse-Database': dbInfo?.db,
      'X-ClickHouse-User': dbInfo?.user,
      'X-ClickHouse-Key': dbInfo?.password,
    });

    log('connection constructed', dbInfo);
  }

  getSeesionId() {
    if (this._hasSessionId) {
      return this._sessionIds.shift();
    }
    return factoryId();
  }
  returnSessionId(id: string) {
    if (this._hasSessionId) {
      this._sessionIds.push(id);
    }
  }

  isClosed() {
    return this._pool && this._pool.closed;
  }
  /**
   * Initiates a new connection pool
   * @param url
   * @param options
   * @returns
   */
  open(url: string | URL, options?: Pool.Options) {
    if (this._pool) {
      log('connection already open');
      return;
    }

    const {connections = 128} = options || {};

    this._hasSessionId = connections !== null;
    if (this._hasSessionId) {
      this._sessionIds = Array(connections)
        .fill('')
        .map(() => factoryId());
    }
    log('connection opening');
    log('pool url', url);
    this._pool = new (undici().Pool)(url, {
      keepAliveMaxTimeout: 600e3,
      bodyTimeout: null,
      headersTimeout: null,
      ...options,
    });
  }

  async getData(resp: Dispatcher.ResponseData['body']) {
    log('getData %o', resp);
    try {
      const results = await resp.json();
      return {...results, status: 'ok', type: 'json'};
    } catch (error) {
      return {status: 'ok', type: 'plain', txt: resp.text};
    }
  }

  /**
   * Perform a request
   * @param method
   * @param endpoint
   * @param headers
   * @param body
   * @returns
   */
  async raw<R>(
    method: 'POST' | 'GET',
    endpoint: string,
    headers?: Dispatcher.DispatchOptions['headers'],
    body?: Dispatcher.DispatchOptions['body'],
  ) {
    assertHasPool(this._pool);

    const len = body
      ? {'content-length': Buffer.byteLength(body as string)}
      : {};
    const passedHeaders = {
      'Content-Type': 'application/json',
      ...(len as IncomingHttpHeaders),
      ...(this._dbInfoHeaders as IncomingHttpHeaders),
      ...headers,
    };
    log('RAW %s', endpoint);
    log('RAW method %s', method);
    log('RAW body %O', body);
    log('RAW headers %O', passedHeaders);

    return this._pool
      .request({
        path: endpoint,
        method: method,
        headers: passedHeaders,
        body: body,
      })
      .then(async ({body, statusCode}) => {
        log('getData');
        const txt = await body.text();
        if (statusCode !== 200) {
          const e = await getErrorObj({
            statusCode: statusCode,
            txt,
          });
          return Promise.reject({error: e});
        }
        if (!txt) {
          return {status: 'ok', type: 'plain', txt: ''};
        }
        if (txt.trim() === OK) {
          return {status: 'ok', type: 'plain', txt: OK};
        }

        try {
          const res = JSON.parse(txt);
          return {...res, status: 'ok', type: 'json'};
        } catch (_ignore) {
          debug("can't parse response as JSON");
          debug('%o', _ignore);
          debug('txt %s', txt);
          return {status: 'ok', type: 'plain', txt: txt};
        }
      });
  }

  /**
   * Perform a POST request
   * @param endpoint
   * @param body
   * @param headers
   * @returns
   */
  post<R>(
    endpoint: string,
    body?: Dispatcher.DispatchOptions['body'],
    headers?: Dispatcher.DispatchOptions['headers'],
  ) {
    log('POST %s', endpoint);
    return this.raw<R>('POST', endpoint, headers, body);
  }

  /**
   * Perform a GET request
   * @param endpoint
   * @param body
   * @param headers
   * @returns
   */
  get<R>(path: string, headers?: Dispatcher.DispatchOptions['headers']) {
    log('GET %s', path);
    return this.raw<R>('GET', path, headers);
  }

  /**
   * Close the connections
   */
  close() {
    if (this._pool) {
      this._pool.close(() => {
        // ignore close errors
      });
    }

    this._pool = undefined;
  }
}
