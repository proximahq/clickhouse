import {expect, test} from 'vitest';

test('sanity', () => {
  expect(1).toBe(1);
});

test('deep', () => {
  expect({
    a: 1,
    hello: 'world',
  }).deep.equal({
    hello: 'world',
    a: 1,
  });
});
