export const TRAILING_SEMI = /;+$/;
export const JSON_SUFFIX = 'FORMAT JSON;';
export const JSON_EACH_SUFFIX = 'FORMAT JSONEachRow';

export const defaultOpts = {
  host: 'localhost',
  port: 8123,
  db: 'default',
  protocol: 'http',
  user: '',
  password: '',
  connections: 1024,
  keepAliveMaxTimeout: 128,
  headersTimeout: 0,
  bodyTimeout: 0,
  size: 128,
};
