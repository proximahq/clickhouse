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
  connections: 2,
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

test('query works as expected', async () => {
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

test('query may insert and parse json values as well', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  const list = [
    'DROP TABLE IF EXISTS bar',
    `CREATE TABLE bar
    (
        \`a\` Int8,
        \`b\` String,
        \`c\` Int8
    )
    ENGINE = MergeTree()
    ORDER BY a;
    `,
    `INSERT INTO bar (*) VALUES (1, 'a', 1),(2, 'b', 2);`,
  ];
  for (const query of list) {
    const r = await client.query(query);

    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }
  const jsonQuery = `SELECT * FROM bar FORMAT JSON`;
  const res = await client.query(jsonQuery);
  expect(res.status).toBe('ok');
  expect(res.rows).toBe(2);
  expect(res.meta).eqls([
    {
      name: 'a',
      type: 'Int8',
    },
    {
      name: 'b',
      type: 'String',
    },
    {
      name: 'c',
      type: 'Int8',
    },
  ]);
  expect(res.data).eqls([
    {
      a: 1,
      b: 'a',
      c: 1,
    },
    {
      a: 2,
      b: 'b',
      c: 2,
    },
  ]);
  await client.close();
});

test('query sanitizes the params', async () => {
  const client = clickhouse({...config, db: database});

  await client.open();
  const list = [
    'DROP TABLE IF EXISTS qux',
    `CREATE TABLE qux
    (
        \`a\` Int8,
        \`b\` String,
        \`c\` Int8
    )
    ENGINE = MergeTree()
    ORDER BY a;
    `,
    `INSERT INTO qux (*) VALUES (1, 'a', 1),(2, 'b', 2);`,
  ];

  for (const query of list) {
    const r = await client.query(query);

    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }

  const res = await client.query(`SELECT * FROM qux WHERE a=? FORMAT JSON`, [
    1,
  ]);
  expect(res.status).toBe('ok');
  expect(res.rows).toBe(1);
  expect(res.data).eqls([
    {
      a: 1,
      b: 'a',
      c: 1,
    },
  ]);
  await client.close();
});

test('query passes JSONEachRow', async () => {
  const client = clickhouse({...config, db: database});

  await client.open();
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
