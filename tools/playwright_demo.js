const { chromium } = require('playwright');
const path = require('path');

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage();
  const demoPath = 'file://' + path.join(process.cwd(), 'demo', 'index.html');
  await page.goto(demoPath);

  await page.click('#connect');
  await page.click('#sub');
  await page.fill('#payload', 'hello-playwright');
  await page.click('#pub');

  // wait for the log to contain our payload
  await page.waitForFunction(() => document.getElementById('log') && document.getElementById('log').innerText.includes('hello-playwright'), { timeout: 8000 });

  console.log('ok');
  await browser.close();
  process.exit(0);
})();
