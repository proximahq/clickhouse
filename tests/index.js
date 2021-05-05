const test = require('ava');
const clickhouse = require('../src');

const database = `test_${Date.now().toString(16)}`;

const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
};

const client = clickhouse({...config, db: database});

test.before(async t => {
  const ch = clickhouse(config);
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
  await ch.query(`CREATE DATABASE ${database}`);
});

test.after(async t => {
  const ch = clickhouse(config);
  await ch.query(`DROP DATABASE IF EXISTS ${database}`);
});

test('query works as expected', async t => {
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

    t.deepEqual(r, {
      status: 'ok',
    });
  }
});

test('selectJson is working as expected', async t => {
  const list = [
    'DROP TABLE IF EXISTS test_json',
    `CREATE TABLE test_json
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
    `INSERT INTO test_json (*) VALUES (1, 'hello', 'world', 1, '2021-01-01 00:00:00'),(2, 'foo', 'bar', 2, '2021-02-02 00:00:00');`,
  ];

  for (const query of list) {
    const r = await client.query(query);

    t.deepEqual(r, {
      status: 'ok',
    });
  }

  const res = await client.selectJson('select * from test_json');
  t.deepEqual(res.data, [
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
    `select a,b from test_json WHERE a=? and b=?;`,
    [1, 'hello'],
  );

  t.deepEqual(withParams.data, [
    {
      a: 1,
      b: 'hello',
    },
  ]);
});

test('query may insert and parse json values', async t => {
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

    t.deepEqual(r, {
      status: 'ok',
    });
  }
  const jsonQuery = `SELECT * FROM bar FORMAT JSON`;

  const res = await client.query(jsonQuery);
  t.is(res.status, 'ok');
  t.is(res.rows, 2);
  t.deepEqual(res.meta, [
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
  t.deepEqual(res.data, [
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

test('query sanitizes the params', async t => {
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

    t.deepEqual(r, {
      status: 'ok',
    });
  }
  const res = await client.query(`SELECT * FROM qux WHERE a=? FORMAT JSON`, [
    1,
  ]);
  t.is(res.status, 'ok');
  t.is(res.rows, 1);
  t.deepEqual(res.data, [
    {
      a: 1,
      b: 'a',
      c: 1,
    },
  ]);
});

test('insert batch', async t => {
  const list = [
    'DROP TABLE IF EXISTS batch',
    `CREATE TABLE batch
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

    t.deepEqual(r, {
      status: 'ok',
    });
  }
  const items = [
    {a: 1, b: 'foo', c: 3},
    {a: 1, b: 'baz', c: 3},
    {a: 1, b: 'bar', c: 3},
  ];
  const insert = () =>
    new Promise((res, rej) => {
      client.insertBatch(
        {
          table: 'batch',
          items,
        },
        {onSuccess: r => res(r)},
      );
    });
  await insert();
  const res = await client.query(`SELECT * FROM batch  FORMAT JSON`);

  t.is(res.status, 'ok');
  t.is(res.rows, 3);
  t.deepEqual(res.data, items);
});

test('insert batch handles errors', async t => {
  const items = [
    {a: 1, b: 'foo', c: 3},
    {a: 1, b: 'baz', c: 3},
    {a: 1, b: 'bar', c: 3},
  ];
  const insert = () =>
    new Promise((res, rej) => {
      client.insertBatch(
        {
          table: 'that_doesnot_exist',
          items,
        },
        {onSuccess: r => res(r), onError: r => rej(r)},
      );
    });
  try {
    await insert();
    t.fail();
  } catch (e) {
    t.pass();
    t.is(e.error.message, '404: Not Found');
  }
});
