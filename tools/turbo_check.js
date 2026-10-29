// Drive the real Overpass Turbo (overpass-turbo.eu) in headless Chromium against our server and
// report what it requested and rendered.
//   node tools/turbo_check.js http://127.0.0.1:8080/api/ out.png
const { chromium } = require('playwright');

const server = process.argv[2] || 'http://127.0.0.1:8080/api/';
const shot = process.argv[3] || 'turbo.png';
const query = `[out:json][timeout:25];
(
  node["amenity"="drinking_water"](52.51,13.38,52.525,13.41);
  way["amenity"="drinking_water"](52.51,13.38,52.525,13.41);
  way["highway"="primary"](52.51,13.38,52.525,13.41);
);
out body;
>;
out skel qt;`;

(async () => {
  const proxy = process.env.HTTPS_PROXY ? { server: process.env.HTTPS_PROXY, bypass: '127.0.0.1,localhost' } : undefined;
  const browser = await chromium.launch({ proxy, args: ['--ignore-certificate-errors'] });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  const seen = [];
  page.on('response', r => { if (r.url().includes('interpreter') || r.status() >= 400) seen.push(`${r.status()} ${r.request().method()} ${r.url().slice(0, 80)}`); });
  page.on('console', m => { if (m.type() === 'error') console.log('console error:', m.text().slice(0, 200)); });
  // Turbo keeps its settings in localStorage under the "overpass-ide_" prefix
  await page.goto('https://overpass-turbo.eu/', { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.evaluate(s => {
    localStorage.setItem('overpass-ide_server', s);  // stored as a raw string
    localStorage.setItem('overpass-ide_code', JSON.stringify({ overpass: '' }));
  }, server);
  const url = 'https://overpass-turbo.eu/?Q=' + encodeURIComponent(query) + '&C=52.517;13.395;16&R';
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(20000);
  const stats = await page.evaluate(() => {
    const el = document.querySelector('#data_stats') || document.querySelector('.stats');
    return el ? el.innerText : '(no stats element)';
  });
  const features = await page.evaluate(() => document.querySelectorAll('path.leaflet-interactive, .leaflet-marker-icon').length);
  const errors = await page.evaluate(() => [...document.querySelectorAll('.modal, .dialog, .error')].map(e => e.innerText.slice(0, 300)).join(' | '));
  await page.screenshot({ path: shot });
  console.log(JSON.stringify({ requests: seen, stats, rendered_features: features, dialogs: errors }, null, 1));
  await browser.close();
})().catch(e => { console.error('FAILED', e.message); process.exit(1); });
