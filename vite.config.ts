/// <reference types="vitest" />

// Configure Vitest (https://vitest.dev/config)

import {defineConfig} from 'vite';
import {vitestTypescriptAssertPlugin} from 'vite-plugin-vitest-typescript-assert';

export default defineConfig({
  plugins: [vitestTypescriptAssertPlugin()],
  test: {
    /* for example, use global to avoid globals imports (describe, test, expect): */
    // globals: true,
  },
});
