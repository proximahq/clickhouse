const http = require('http');
const https = require('https');
const qs = require('querystring');
const sqlString = require('sqlstring');

const fn = _ => _;
const TRAILING_SEMI = /;+$/;
const JSON_SUFFIX = 'FORMAT JSON;';
const JSON_EACH_SUFFIX = 'FORMAT JSONEachRow';
const ERRORED = new RegExp('Code: ([0-9]{2}), .*Exception:');

const cleanup = str => str.replace(TRAILING_SEMI, '');

function getErrorObj(res) {
  const err = new Error(`${res.statusCode}: ${res.body || res.statusMessage}`);

  if (res.body) {
    const m = res.body.match(ERRORED);
    if (m) {
      if (m[1] && isNaN(parseInt(m[1])) === false) {
        err.code = parseInt(m[1]);
      }

      if (m[2]) {
        err.message = m[2];
      }
    }
  }

  return err;
}

const log = require('debug')('clickhouse');

const req = ({
  protocol,
  user,
  password,
  host,
  db,
  path,
  port,
  body = '',
  onSuccess = fn,
  onError = fn,
}) => {
  const handler = protocol === 'http' ? http : https;
  const p = path ?? '/';
  const authHeaders =
    user || password ? {auth: `${user ?? 'default'}:${password ?? ''}`} : {};

  const options = {
    hostname: host,
    port,
    path: p,
    method: 'POST',
    headers: {
      'X-ClickHouse-Database': db,
      'Content-Type': 'application/json',
    },
    ...authHeaders,
  };

  return handler
    .request(options, res => {
      let data = '';
      res.on('data', d => {
        data += d;
      });
      res
        .on('end', () => {
          if (res.statusCode === 200) {
            try {
              const results = JSON.parse(data);
              onSuccess({...results, status: 'ok'});
            } catch (ingore) {
              onSuccess({status: 'ok'});
            }
          } else {
            const e = getErrorObj(res);
            onError({error: e});
          }
        })
        .on('error', err => {
          onError(err);
        });
    })
    .end(body);
};

const defaultOpts = {
  host: 'localhost',
  port: 8123,
  db: 'default',
  protocol: 'http',
  user: '',
  password: '',
};

/**
 * @typedef {Object} Client
 * @property {function(): import('http').ClientRequest} insertBatch - Insert batch clickhouse items
 * @property {function(): Promise} query - Query over the HTTP interface
 * @property {function(): Promise} selectJson - Async query JSON results
 */

/**
 * @param {Object} options
 * @return {Client}
 */

const clickhouse = (opts = {}) => {
  log('init');

  const options = {...defaultOpts, ...opts};

  const instance = {
    query: (query = '', params = [], extras = {}) => {
      const executableQuery = `${sqlString.format(query, params)};`;
      return new Promise((res, rej) => {
        req({
          ...options,
          ...extras,
          body: executableQuery,
          onError: rej,
          onSuccess: res,
        });
      });
    },

    selectJson: (query = '', params = [], extras = {}) => {
      const q = cleanup(query);
      const executableQuery = `${sqlString.format(q, params)} ${JSON_SUFFIX};`;

      return new Promise((res, rej) => {
        req({
          ...options,
          ...extras,
          body: executableQuery,
          onError: rej,
          onSuccess: res,
        });
      });
    },

    insertBatch: ({table, items = []}, extras = {}) => {
      if (!table) {
        throw new Error('`table` is required for batch insert');
      }
      if (!items) {
        throw new Error('`items` are required for batch insert');
      }

      const path = `/?${qs.stringify({
        query: `INSERT INTO ${table} ${JSON_EACH_SUFFIX}`,
      })}`;
      return req({...options, body: JSON.stringify(items), path, ...extras});
    },
  };

  return instance;
};

module.exports = clickhouse;
