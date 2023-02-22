import {expect, test} from 'vitest';
import {expectType} from 'vite-plugin-vitest-typescript-assert/tsd';

// Ts checking
export const tested = {
  hello: 'world',
  life: 42,
  data: {
    id: 385643984,
    items: [1, 2, 3],
  },
};

test('sanity', () => {
  expect(1).toBe(1);
});

test('tsd', () => {
  expectType<string>(tested.hello);
  expectType<typeof tested>({
    hello: 'you',
    life: 42,
    data: {
      id: 385643984,
      items: [1, 2, 3],
    },
  });
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
