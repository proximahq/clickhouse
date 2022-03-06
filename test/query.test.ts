import {expect, test, beforeAll, afterAll} from 'vitest';
import clickhouse from '../src';
import {dbName} from './utils';

const database = dbName();

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

test('query works as expected', async () => {
  const client = clickhouse({...config, db: database});
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
    `
  			SELECT
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
});

test('query may insert and parse json values as well', async () => {
  const client = clickhouse({...config, db: database});
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
});

test('query sanitizes the params', async () => {
  const client = clickhouse({...config, db: database});

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
});
