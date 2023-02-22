import {clickhouse} from '../../src';

const database = `test_db`;

const config = {
  port: 8123,
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'test',
};

const client = clickhouse({...config, db: database});

const run = async () => {
  try {
    const res = await client.query('show databases FORMAT JSON');
    const p = await client.ping();
    console.log(res);
    console.log(p);
  } catch (error) {
    console.log(error);
  }
};

run();
