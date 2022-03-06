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

test('insert batch throws', async () => {
  const client = clickhouse({...config, db: database});
  await expect(() =>
    client.insertBatch('foo'),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`table` is required for batch insert"',
  );

  await expect(() =>
    client.insertBatch({table: 'foo'}),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`items` are required for batch insert"',
  );
});

test.skip('insert batch works', async () => {
  const client = clickhouse({...config, db: database});
  const list = [
    'DROP TABLE IF EXISTS batchit',
    `CREATE TABLE batchit
      (
          \`a\` Int8,
          \`b\` String,
          \`c\` Int8
      )
      ENGINE = MergeTree()
      ORDER BY a;
    `,
  ];
  for (const query of list) {
    const r = await client.query(query);

    await expect(r).eqls({
      txt: '',
      status: 'ok',
      type: 'plain',
    });
  }
  const items = [
    {a: 1, b: 'foo', c: 3},
    {a: 1, b: 'baz', c: 3},
    {a: 1, b: 'bar', c: 3},
  ];
  const r = await client.insertBatch({
    table: 'batchit',
    items,
  });
  const res = await client.query(`SELECT * FROM batchit  FORMAT JSON`);
  expect(res.status).toBe('ok');
  expect(res.rows).toBe(3);
  expect(res.type).toBe('json');
  expect(res.data).eqls(items);
});

test('insert batch handles errors', async () => {
  const client = clickhouse({...config, db: database});
  const items = [
    {a: 1, b: 'foo', c: 3},
    {a: 1, b: 'baz', c: 3},
    {a: 1, b: 'bar', c: 3},
  ];

  try {
    await client.insertBatch({
      table: 'that_doesnot_exist',
      items,
    });
    expect.fail();
  } catch (e) {
    expect(e.statusCode).toBe(404);
  }
});
