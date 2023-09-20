import {expect, test, beforeAll, afterAll} from 'vitest';
import {clickhouse} from '../src';
import {dbName} from './utils';
import Stream from 'stream';
import path from 'path';
import fs from 'fs';

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

test('insertStream throws', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  await expect(() =>
    // @ts-ignore
    client.insertStream('foo'),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`table` is required for batch insert"',
  );

  await expect(() =>
    // @ts-ignore
    client.insertStream({
      table: 'foo',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    '"`items` are required for batch insert"',
  );

  await client.close();
});

test('insertStream works for named CSV', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  const list = [
    'DROP TABLE IF EXISTS stream_csv_named',
    `CREATE TABLE stream_csv_named
        (
            timestamp DateTime default now(),
            a String,
            b String,
            c Int
        )
        ENGINE = MergeTree()
        ORDER BY timestamp;
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

  await client.insertStream({
    table: 'stream_csv_named',
    format: 'CSVWithNames',
    items: fs.createReadStream(
      path.join(__dirname, 'fixtures', 'csv_named.csv'),
    ),
  });

  const res = await client.selectJson('SELECT * FROM stream_csv_named');
  expect(res.data).toMatchSnapshot();
  await client.close();
});

test('insertStream works for plain CSV', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  const list = [
    'DROP TABLE IF EXISTS stream_csv_plain',
    `CREATE TABLE stream_csv_plain
        (
            timestamp DateTime default now(),
            a String,
            b String,
            c Int
        )
        ENGINE = MergeTree()
        ORDER BY timestamp;
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

  await client.insertStream({
    table: 'stream_csv_plain',
    format: 'CSV',
    items: fs.createReadStream(path.join(__dirname, 'fixtures', 'csv.csv')),
  });

  const res = await client.selectJson(
    'SELECT * FROM stream_csv_plain ORDER BY timestamp',
  );
  expect(res.data).toMatchSnapshot();
  await client.close();
});

test('insertStream works for TSV', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();
  const list = [
    'DROP TABLE IF EXISTS stream_tsv_with_names',
    `CREATE TABLE stream_tsv_with_names
        (
            id Int,
            timestamp Datetime default now(),
            value Int
        )
        ENGINE = MergeTree()
        ORDER BY timestamp;
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
  const headers = `id\ttimestamp\tvalue\n`;
  const values = `42\t2013-11-21 20:00:00\t22\n43\t2013-11-21 20:00:01\t55\n`;

  const rawStream = Stream.Readable.from(values, {
    objectMode: false,
  });
  const streamWithNames = Stream.Readable.from(headers + values, {
    objectMode: false,
  });

  await client.insertStream({
    table: 'stream_tsv_with_names',
    format: 'TabSeparatedWithNames',
    items: streamWithNames,
  });

  await client.insertStream({
    table: 'stream_tsv_with_names',
    format: 'TabSeparated',
    items: rawStream,
  });

  const res = await client.selectJson(
    'SELECT * FROM stream_tsv_with_names ORDER BY timestamp',
  );
  expect(res.data).toMatchSnapshot();
  await client.close();
});

test('insertStream works for multiple streams', async () => {
  const client = clickhouse({...config, db: database});
  await client.open();

  const values = `42\t2013-11-21 20:00:00\t22\n43\t2013-11-21 20:00:01\t55\n`;
  const list = [
    'DROP TABLE IF EXISTS stream_multi',
    `CREATE TABLE stream_multi
      (
          id Int,
          timestamp Datetime default now(),
          value Int
      )
      ENGINE = MergeTree()
      ORDER BY timestamp;
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

  await Promise.all([
    client.insertStream({
      table: 'stream_multi',
      format: 'TabSeparated',
      items: Stream.Readable.from(values, {
        objectMode: false,
      }),
    }),
    client.insertStream({
      table: 'stream_multi',
      format: 'TabSeparated',
      items: Stream.Readable.from(values, {
        objectMode: false,
      }),
    }),
  ]);

  const res = await client.selectJson(
    'SELECT * FROM stream_multi ORDER BY timestamp',
  );
  expect(res.data).toMatchSnapshot();
  await client.close();
});
