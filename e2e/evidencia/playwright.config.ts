import { defineConfig } from '@playwright/test';

const size = { width: 1280, height: 720 };

export default defineConfig({
  testDir: '.',
  testMatch: '*.spec.ts',
  outputDir: 'test-results',
  workers: 1,
  fullyParallel: false,
  retries: 0,
  timeout: 15 * 60 * 1000,
  reporter: [['list']],
  use: {
    viewport: size,
    video: { mode: 'on', size },
    launchOptions: { slowMo: 600 },
  },
});
