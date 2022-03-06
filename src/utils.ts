import {Pool} from 'undici';
import {ClickhouseOptions} from './types';
export const TRAILING_SEMI = /;+$/;
export const JSON_SUFFIX = 'FORMAT JSON;';
export const JSON_EACH_SUFFIX = 'FORMAT JSONEachRow';

export const fn = (...args: any[]): void => {};
export const defaultOpts = {
  host: 'localhost',
  port: 8123,
  db: 'default',
  protocol: 'http',
  user: '',
  password: '',
  connections: null,
};

export const cleanup = (str: string) => str.replace(TRAILING_SEMI, '');

let undiciPool: Pool;

export const getUndici = ({host, port, protocol, connections}): Pool => {
  if (undiciPool) return undiciPool;
  const u = `${protocol}://${host}:${port}`;
  undiciPool = new Pool(u, {
    connections: connections ?? 1,
  });
  return undiciPool;
};

export const grabInstance = () => {
  return undiciPool;
};

export const bumpHeaders = ({
  user,
  password,
}: Pick<ClickhouseOptions, 'user' | 'password'>) => ({
  'Content-Type': 'application/json',
  ...(user && {'X-ClickHouse-User': user}),
  ...(password && {'X-ClickHouse-Key': password}),
});
