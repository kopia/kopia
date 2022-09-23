import { test, expect } from '@playwright/test'
import { 
   findLatestBuild, 
   parseElectronApp } from 'electron-playwright-helpers'
 import { _electron as electron } from 'playwright'

 import path from 'path';

 let electronApp

 function getKopiaUIBuiltPath() {
  switch (process.platform + "/" + process.arch) {
    case "darwin/x64":
      return path.resolve("../dist/kopia-ui/mac");
    case "darwin/arm64":
      return path.resolve("../dist/kopia-ui/mac-arm64");
    case "linux/x64":
      return path.resolve("../dist/kopia-ui/linux-unpacked");
    case "linux/arm64":
      return path.resolve("../dist/kopia-ui/linux-arm64-unpacked");
    case "win32/x64":
      return path.resolve("../dist/kopia-ui/win-unpacked");
    default:
      return null;
  }
 }
 
 test.beforeAll(async () => {
   const latestBuild = getKopiaUIBuiltPath();
   expect(latestBuild).not.toBeNull();

   // parse the directory and find paths and other info
   const appInfo = parseElectronApp(latestBuild)
   console.log('appInfo', appInfo);
   process.env.CI = 'e2e'
   process.env.KOPIA_UI_TESTING = '1'
   electronApp = await electron.launch({
     args: [appInfo.main],
     executablePath: appInfo.executable
   })
   electronApp.on('window', async (page) => {
     const filename = page.url()?.split('/').pop()
     console.log(`Window opened: ${filename}`)
 
     // capture errors
     page.on('pageerror', (error) => {
       console.error(error)
     })
     // capture console messages
     page.on('console', (msg) => {
       console.log(msg.text())
     })
   })
 })
 
 test.afterAll(async () => {
   await electronApp.close()
 })
 
 test('renders the first page', async () => {
  await electronApp.evaluate(async ({app}) => {
    app.testHooks.showRepoWindow('repository');
  });
  const page = await electronApp.firstWindow();

  expect(page).toBeTruthy();
  expect(await page.title()).toMatch(/KopiaUI v\d+/);

  // TODO - we can exercise some UI scenario using 'page'

  await electronApp.evaluate(async ({app}) => {
    return app.testHooks.tray.closeContextMenu();
  })
});
