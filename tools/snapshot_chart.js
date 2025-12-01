import { chromium } from '@playwright/test';

async function main() {
  const [url, outPath] = process.argv.slice(2);
  if (!url || !outPath) {
    console.error('Usage: node tools/snapshot_chart.js <url> <out-path>');
    process.exit(1);
  }

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1366, height: 900 } });
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForSelector('.viz', { timeout: 8000 }).catch(() => {});
  await page.waitForTimeout(1500);
  await page.screenshot({ path: outPath, fullPage: true });
  await browser.close();
}

main();
