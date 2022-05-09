import {expect, test, beforeAll, afterAll} from 'vitest';
import {clickhouse} from '../src';
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
  port: 8123,
  password: 'password',
  connections: 10,
};

beforeAll(async t => {
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

afterAll(async t => {
  const ch = clickhouse(config);
  await ch.open();
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
  await ch.close();
});

test('insert batch throws', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
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
  await client.close();
});

test('insert batch works', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
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
  await client.close();
});

test('insert batch handles errors', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
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
    expect(e.error.statusCode).toBe(404);
  }

  await client.close();
});
