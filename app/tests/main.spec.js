import { test, expect } from '@playwright/test'
import { _electron as electron } from 'playwright'

import path from 'path';

let electronApp

function getKopiaUIUnpackedDir() {
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
 
function getMainPath(unpackedDir) {
  switch (process.platform) {
    case "darwin":
      return path.join(unpackedDir, "KopiaUI.app", "Contents", "Resources", "app.asar", "public", "electron.js");
    default:
      return path.join(unpackedDir, "resources", "app.asar", "public", "electron.js");
  }
}

function getExecutablePath(unpackedDir) {
  switch (process.platform) {
    case "win32":
      return path.join(unpackedDir, "KopiaUI.exe");
    case "darwin":
      return path.join(unpackedDir, "KopiaUI.app", "Contents", "MacOS", "KopiaUI");
    default:
      return path.join(unpackedDir, "kopia-ui");
  }
}

test.beforeAll(async () => {
  const unpackedDir = getKopiaUIUnpackedDir();
  expect(unpackedDir).not.toBeNull();

  const mainPath = getMainPath(unpackedDir);
  const executablePath = getExecutablePath(unpackedDir);

  console.log('main path', mainPath);
  console.log('executable path', executablePath);

  process.env.CI = 'e2e'
  process.env.KOPIA_UI_TESTING = '1'
  electronApp = await electron.launch({
    args: [mainPath],
    executablePath: executablePath,
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
 
test('opens repository window', async () => {
  await electronApp.evaluate(async ({app}) => {
    app.testHooks.showRepoWindow('repository');
  });

  const page = await electronApp.firstWindow();

  expect(page).toBeTruthy();
  await page.waitForNavigation({waitUntil: 'networkidle', networkIdleTimeout: 1000});
  expect(await page.title()).toMatch(/KopiaUI v\d+/);

  // TODO - we can exercise some UI scenario using 'page'

  await electronApp.evaluate(async ({app}) => {
    return app.testHooks.tray.popUpContextMenu();
  })

  await electronApp.evaluate(async ({app}) => {
    return app.testHooks.tray.closeContextMenu();
  })
});
