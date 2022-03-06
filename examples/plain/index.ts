import clickhouse from '../../src';

const database = `test_${Date.now().toString(16)}`;

const config = {
  port: 8123,
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
};

const client = clickhouse({...config, db: database});

const run = async () => {
  try {
    const res = await client.query('DROP TABLE IF EXISTS test_json');
    const p = await client.ping();
    console.log(p);
  } catch (error) {
    console.log(error);
  }
};

run();
