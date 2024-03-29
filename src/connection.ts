import {Readable, Writable} from 'stream';
import {EventEmitter} from 'node:events';
import type {Dispatcher, Pool} from 'undici';
import {IncomingHttpHeaders} from 'http';
import dbg from 'debug';
import {debug} from 'console';
import type {URL} from 'url';
import {getErrorObj} from './error';
import {cleanupObj, genIds} from './utils';
import {OK} from './constants';

const log = dbg('proxima:clickhouse-driver:connection');

class PoolEmitter extends EventEmitter {}

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
  private emitter: PoolEmitter = new PoolEmitter();
  private _sessionIds: string[] = [];
  private _isInFallbackMode = false;
  private _hasSessionId: boolean = true;

  private _dbInfoHeaders: Pool | {};

  constructor(dbInfo: DbIfo = {}) {
    this._dbInfoHeaders = cleanupObj({
      'Accept-Encoding': '*',
      'X-ClickHouse-Database': dbInfo?.db,
      'X-ClickHouse-User': dbInfo?.user,
      'X-ClickHouse-Key': dbInfo?.password,
    });

    log('connection constructed', this._dbInfoHeaders);
  }

  getFallbackMode() {
    return this._isInFallbackMode;
  }

  setFallbackMode() {
    this._isInFallbackMode = true;
  }

  removeFallbackMode() {
    this._isInFallbackMode = false;
  }

  getSessionId() {
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

  on(eventName: string, cb: (...args: any[]) => void) {
    log('on event: %s', eventName);
    this.emitter.on(eventName, cb);
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
      this._sessionIds = Array.from({length: connections || 0}).map(() =>
        factoryId(),
      );
    }

    log('connection opening');
    log('pool url', url);

    this._pool = new (undici().Pool)(url, {
      keepAliveMaxTimeout: 600e3,
      bodyTimeout: 0,
      headersTimeout: null,
      ...options,
    });

    this._pool?.on('connect', () => {
      this.emitter.emit('connect');
      this.removeFallbackMode();
      log('connection opened');
    });
    this._pool?.on('connectionError', () => {
      this.emitter.emit('connectionError');
      this.setFallbackMode();
      log('connection error');
    });
    this._pool?.on('disconnect', () => {
      this.emitter.emit('disconnect');
      log('connection closed');
    });

    this._pool?.on('drain', () => {
      this.emitter.emit('drain');
    });

    return this._pool;
  }

  async getData(resp: Dispatcher.ResponseData['body']) {
    log('getData %o', resp);
    try {
      const results = (await resp.json()) as any;
      return {...results, status: 'ok', type: 'json'};
    } catch (error) {
      return {status: 'ok', type: 'plain', txt: resp.text};
    }
  }

  /**
   * Perform a streaming request
   * @param endpoint
   * @param headers
   * @param stream with data to send
   * @returns
   */

  async stream(
    endpoint: string,
    headers?: Dispatcher.DispatchOptions['headers'],
    stream?: Readable,
  ) {
    assertHasPool(this._pool);

    const passedHeaders = {
      'Content-Type': 'application/json',
      ...(this._dbInfoHeaders as IncomingHttpHeaders),
      ...headers,
    };
    const bufs = [] as Buffer[];
    return this._pool?.stream(
      {
        path: endpoint,
        method: 'POST',
        headers: passedHeaders,
        body: stream,
        opaque: {bufs},
        throwOnError: true,
      },
      ({statusCode}) => {
        log('stream %s', endpoint);
        if (statusCode !== 200) {
          throw new Error(`Unexpected status code ${statusCode}`);
        }
        return new Writable({
          write(chunk, _encoding, callback) {
            bufs.push(chunk);
            callback();
          },
        });
      },
    );
  }

  /**
   * Perform a request
   * @param method
   * @param endpoint
   * @param headers
   * @param body
   * @returns
   */
  async raw<Results>(
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

    const resp = await this._pool.request({
      path: endpoint,
      method: method,
      headers: passedHeaders,
      body: body,
    });

    log('getData');
    const txt = await resp.body.text();
    if (resp.statusCode !== 200) {
      const e = await getErrorObj({
        statusCode: resp.statusCode,
        txt,
      });

      return Promise.reject({error: e});
    }

    if (!txt || txt.trim() === '') {
      return {status: 'ok', type: 'plain', txt: ''};
    }

    if (txt.trim() === OK) {
      return {status: 'ok', type: 'plain', txt: OK};
    }

    try {
      const res = JSON.parse(txt);
      return {...res, status: 'ok', type: 'json'} as Results;
    } catch (_ignore) {
      debug("can't parse response as JSON");
      debug('%o', _ignore);
      debug('txt %s', txt);
      return {status: 'ok', type: 'plain', txt: txt};
    }
  }

  /**
   * Perform a POST request
   * @param endpoint
   * @param body
   * @param headers
   * @returns
   */
  post<Results>(
    endpoint: string,
    body?: Dispatcher.DispatchOptions['body'],
    headers?: Dispatcher.DispatchOptions['headers'],
  ) {
    if (this.getFallbackMode()) {
      return Promise.reject({
        error: new Error('Connection is in fallback mode'),
      });
    }
    log('POST %s', endpoint);
    return this.raw<Results>('POST', endpoint, headers, body);
  }

  /**
   * Perform a GET request
   * @param endpoint
   * @param body
   * @param headers
   * @returns
   */
  get<Results>(path: string, headers?: Dispatcher.DispatchOptions['headers']) {
    log('GET %s', path);
    return this.raw<Results>('GET', path, headers);
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
