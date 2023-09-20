import {test} from 'vitest';
import {clickhouse} from '../src';

const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  port: 8123,
  password: 'password',
  connections: 10,
};

test('should work on open', () =>
  new Promise<void>(async done => {
    const client = clickhouse(config);
    client.on('connect', () => {
      done();
    });
    await client.open();
    await client.query(`select 1`);
    await client.close();
  }));

test('should work on close', () =>
  new Promise<void>(async done => {
    const client = clickhouse(config);
    client.on('disconnect', () => {
      done();
    });
    await client.open();
    await client.query(`select 1`);
    await client.close();
  }));
