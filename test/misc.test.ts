import {expect, test} from 'vitest';
import {clickhouse} from '../src';

const config = {
  host: 'localhost',
  port: 8123,
  connections: 128,
  user: 'default',
  protocol: 'http',
  password: 'password',
  db: 'proxima',
  sessionId: 'helloworld',
};

test('pings', async () => {
  const client = clickhouse({...config});
  await client.open();
  const res = await client.ping();

  expect(res.status).toBe('ok');
  expect(res.type).toBe('plain');
  expect(res.txt).toBe('Ok.');
});
