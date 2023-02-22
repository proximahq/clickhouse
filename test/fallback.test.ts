import {expect, test} from 'vitest';
import {clickhouse} from '../src';

const config = {
  host: 'this_obviously_does_not_exist.xyz',
  protocol: 'http',
  user: 'default',
  port: 8123,
  password: 'password',
  connections: 10,
};

test('with fallback strategy does not throw', async () => {
  const client = clickhouse(config);
  await client.open();
  expect(true).toBe(true);
  await client.close();
});

test('with batch still throws for params', async () => {
  const client = clickhouse(config);
  await client.open();
  await expect(() =>
    // @ts-ignore
    client.insertBatch('foo'),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`table` is required for batch insert"',
  );

  await expect(() =>
    // @ts-ignore
    client.insertBatch({table: 'foo'}),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`items` are required for batch insert"',
  );
});

test('with batch throws when table does not exist', async () => {
  const client = clickhouse(config);
  await client.open();
  await client
    .insertBatch({
      table: 'batchthrow',
      items: [{a: 1}],
    })
    .catch(e => {
      expect(e).toBeDefined();
    });
});

test('with batch throws when table does not exist', async () => {
  const client = clickhouse(config);
  await client.open();
  await client.insertBatch(
    {
      table: 'batchthrow',
      items: [{a: 1}],
    },
    // @ts-ignore
    (q, e) => {
      expect(e).toBeDefined();
      expect(q).toMatchInlineSnapshot(`
        {
          "items": [
            {
              "a": 1,
            },
          ],
          "table": "batchthrow",
        }
      `);
    },
  );
});
