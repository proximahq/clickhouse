import {expect, test, beforeAll, afterAll} from 'vitest';
import {clickhouse} from '../src';
import {dbName} from './utils';

const database = dbName();

const config = {
  host: 'localhost',
  protocol: 'http',
  port: 8123,
  user: 'default',
  password: 'password',
  connections: 128,
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
  try {
    const ch = clickhouse(config);
    await ch.open();
    await ch.query(`DROP DATABASE IF EXISTS ${database}`);
    await ch.close();
  } catch (error) {
    console.log(error);
  }
});

test('query with pool works as expected', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  const list = [
    'DROP TABLE IF EXISTS foo',
    `
      CREATE TABLE foo (
        date Date,
        time DateTime,
        mark String,
        ips Array(UInt32),
        queries Nested (
          act String,
          id UInt32
        )
      )
      ENGINE=MergeTree(date, (mark, time), 8192)`,
    'OPTIMIZE TABLE foo PARTITION 201807 FINAL',
    `CREATE OR REPLACE TABLE test
        (
            id UInt64,
            size_bytes Int64,
            size String Alias formatReadableSize(size_bytes)
        )
        ENGINE = MergeTree
        ORDER BY id
      `,
    `INSERT INTO test Values (1, 4678899);`,
    `SELECT
                    *
                FROM foo
                LIMIT 10
            `,
  ];

  for (const query of list) {
    const r = await client.query(query);
    expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }
  await client.close();
});
