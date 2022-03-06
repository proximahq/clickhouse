import {expect, test, beforeAll, afterAll} from 'vitest';
import clickhouse from '../src';
import {dbName} from './utils';

const database = dbName();

async function streamToJSON(readable) {
  let result = '';
  for await (const chunk of readable) {
    result += chunk;
  }
  return JSON.parse(result);
}

const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
  connections: 2,
};

beforeAll(async t => {
  const ch = clickhouse(config);
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
  await ch.query(`CREATE DATABASE ${database}`);
});

afterAll(async t => {
  const ch = clickhouse(config);
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
});

test('queryStream "streams"', async () => {
  const client = clickhouse({...config, db: database});
  const res = await client.queryStream({
    query: 'SELECT 1 as a, 2 as b FORMAT JSON;',
  });
  const json = await streamToJSON(res.opaque);
  expect(json.data).eqls([
    {
      a: 1,
      b: 2,
    },
  ]);
});
