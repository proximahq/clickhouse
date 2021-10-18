import clickhouse from '../src/index';

const database = `test_${Date.now().toString(16)}`;

const config = {
  port: 8012,
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
};

const client = clickhouse({...config, db: database});

const run = async () => {
  try {
    const res = await client.query('DROP TABLE IF EXISTS test_json');
    console.log(res);
  } catch (error) {
    console.log(error);
  }
};

run();
