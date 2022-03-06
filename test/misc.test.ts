import {expect, test} from 'vitest';
import clickhouse from '../src';

const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
  connections: 2,
};

test('pings', async () => {
  const client = clickhouse({...config});
  const res = await client.ping();
  expect(res.status).toBe('ok');
  expect(res.type).toBe('plain');
  expect(res.txt).toBe('Ok.\n');
});
