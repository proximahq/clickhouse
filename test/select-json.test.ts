import {assert, expect, test, beforeAll, afterAll} from 'vitest';
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

test.skip('selectJson parses the values with right types', async () => {
  const client = clickhouse({...config, db: database});
  const list = [
    'DROP TABLE IF EXISTS test_json_select',
    `CREATE TABLE test_json_select
    (
        \`a\` Int8,
        \`b\` String,
        \`c\` String,
        \`d\` Int8,
        \`timestamp\` DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY a;
    `,
    `
      INSERT INTO test_json_select (*)
      VALUES
        (1, 'hello', 'world', 1, '2021-01-01 00:00:00'),
        (2, 'foo', 'bar', 2, '2021-02-02 00:00:00');`,
  ];

  for (const query of list) {
    const r = await client.query(query);
    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }

  const res = await client.selectJson(
    'select toInt32(count(*)) as counted from test_json_select WHERE a = ?',
    [1],
  );

  await expect(res.data).eqls([
    {
      counted: 1,
    },
  ]);

  const resAsString = await client.selectJson(
    'select count(*) as counted from test_json_select',
  );

  expect(resAsString.data).eqls([
    {
      counted: '2',
    },
  ]);
});

test.skip('selectJson returns all the metadata', async () => {
  const client = clickhouse({...config, db: database});
  const res = await client.selectJson(
    `SELECT 'hello' as a, 'world' as b, 1 as c, 2 as d`,
  );
  expect(res.data).toMatchInlineSnapshot(`
    [
      {
        "a": "hello",
        "b": "world",
        "c": 1,
        "d": 2,
      },
    ]
  `);
  expect(res.meta).toMatchInlineSnapshot(`
    [
      {
        "name": "a",
        "type": "String",
      },
      {
        "name": "b",
        "type": "String",
      },
      {
        "name": "c",
        "type": "UInt8",
      },
      {
        "name": "d",
        "type": "UInt8",
      },
    ]
  `);
  expect(res.status).toBe('ok');
  expect(res.rows).toBe(1);
  expect(res.statistics).toBeDefined();
});

test('selectJson is working as expected', async () => {
  const client = clickhouse({...config, db: database});
  const list = [
    'DROP TABLE IF EXISTS test_json_expected',
    `CREATE TABLE test_json_expected
    (
        \`a\` Int8,
        \`b\` String,
        \`c\` String,
        \`d\` Int8,
        \`timestamp\` DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY a;
    `,
    `INSERT INTO test_json_expected (*) VALUES (1, 'hello', 'world', 1, '2021-01-01 00:00:00'),(2, 'foo', 'bar', 2, '2021-02-02 00:00:00');`,
  ];
  for (const query of list) {
    const r = await client.query(query);
    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }
  const res = await client.selectJson('select * from test_json_expected');
  expect(res.data).eqls([
    {
      a: 1,
      b: 'hello',
      c: 'world',
      d: 1,
      timestamp: '2021-01-01 00:00:00',
    },
    {
      a: 2,
      b: 'foo',
      c: 'bar',
      d: 2,
      timestamp: '2021-02-02 00:00:00',
    },
  ]);
  const withParams = await client.selectJson(
    `select a, b from test_json_expected WHERE a=? and b=?;`,
    [1, 'hello'],
  );
  expect(withParams.data).eqls([
    {
      a: 1,
      b: 'hello',
    },
  ]);
});
