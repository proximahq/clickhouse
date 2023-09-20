import {expect, test, beforeAll, afterAll} from 'vitest';
import {clickhouse} from '../src';
import {dbName} from './utils';

const database = dbName();

const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  port: 8123,
  password: 'password',
  connections: 10,
};

beforeAll(async () => {
  try {
    const ch = clickhouse(config);
    await ch.open();
    await ch.query(`DROP DATABASE IF EXISTS ${database}`);
    await ch.query(`CREATE DATABASE ${database}`);
    await ch.close();
  } catch (error) {
    console.log(error);
  }
});

afterAll(async () => {
  const ch = clickhouse(config);
  await ch.open();
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
  await ch.close();
});

test('errored query keeps the connection open', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();

  try {
    const q = await client.query('SELECT fkdjbasx from jkfdskjhsd;;;;');
  } catch (error) {
    expect(error).toBeDefined();
  }

  //   Keep the connection open and try to query again
  const list = [
    'DROP TABLE IF EXISTS json_each',
    `CREATE TABLE json_each
    (
        \`hi\` Int8,
        \`hello\` String,
        \`bye\` Int8
    )
    ENGINE = MergeTree()
    ORDER BY hi;
    `,
    `INSERT INTO json_each (*) VALUES (1, 'a', 1),(2, 'b', 2);`,
  ];

  for (const query of list) {
    const r = await client.query(query);

    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }

  const res = await client.query(
    `SELECT count(*) as counted FROM json_each FORMAT JSONEachRow`,
  );
  expect(res.counted).toBe(2);
  expect(res.status).toBe('ok');
  expect(res.type).toBe('json');

  await client.close();
});
