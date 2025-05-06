import { test, expect } from '@playwright/test'
import { _electron as electron } from 'playwright'

import fs from 'fs';
import os from 'os';
import path from 'path';

const DEFAULT_REPO_ID = 'repository';

let electronApp
let mainPath
let executablePath
let tmpAppDataDir

function getKopiaUIDir() {
  switch (process.platform + "/" + process.arch) {
    case "darwin/x64":
      return path.resolve("../dist/kopia-ui/mac");
    case "darwin/arm64":
      return path.resolve("../dist/kopia-ui/mac-arm64");
    case "linux/x64":
      // on Linux we must run from installed location due to AppArmor profile
      return path.resolve("/opt/KopiaUI");
    case "linux/arm64":
      // on Linux we must run from installed location due to AppArmor profile
      return path.resolve("/opt/KopiaUI");
    case "win32/x64":
      return path.resolve("../dist/kopia-ui/win-unpacked");
    default:
      return null;
  }
}
 
function getMainPath(kopiauiDir) {
  switch (process.platform) {
    case "darwin":
      return path.join(kopiauiDir, "KopiaUI.app", "Contents", "Resources", "app.asar", "public", "electron.js");
    default:
      return path.join(kopiauiDir, "resources", "app.asar", "public", "electron.js");
  }
}

function getExecutablePath(kopiauiDir) {
  switch (process.platform) {
    case "win32":
      return path.join(kopiauiDir, "KopiaUI.exe");
    case "darwin":
      return path.join(kopiauiDir, "KopiaUI.app", "Contents", "MacOS", "KopiaUI");
    default:
      return path.join(kopiauiDir, "kopia-ui");
  }
}

/**
 * Creates a temporary application data directory along with the kopia
 * directory for testing purposes.
 *
 * @returns {string} The path to the created temporary directory.
 */
function createTemporaryAppDataDir() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'kopia-test-'));
  fs.mkdirSync(path.join(tmpDir, 'kopia'));
  return tmpDir;
}

/**
 * Launches a new instance of the Electron app with the given app data directory.
 *
 * Also captures page errors and console messages and logs them to the console.
 *
 * @param {string} appDataDir - the path to the app data directory
 * @returns {Promise<Electron.App>} - a promise that resolves to the launched app
 */
async function launchApp(appDataDir) {
  const electronApp = await electron.launch({
    args: [mainPath],
    executablePath: executablePath,
    env: {
      ...process.env,
      KOPIA_CUSTOM_APPDATA: appDataDir,
    }
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

  return electronApp;
}

/**
 * Waits for Kopia to start up by delaying for a specified duration.
 *
 * @returns {Promise<void>} A promise that resolves after the delay.
 */
function waitForKopiaToStartup() {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, 2500);
  });
}

test.beforeAll(() => {
  const kopiauiDir = getKopiaUIDir();
  expect(kopiauiDir).not.toBeNull();

  mainPath = getMainPath(kopiauiDir);
  executablePath = getExecutablePath(kopiauiDir);

  console.log('main path', mainPath);
  console.log('executable path', executablePath);

  process.env.CI = 'e2e';
  process.env.KOPIA_UI_TESTING = '1';
})

test.beforeEach(async () => {
  tmpAppDataDir = createTemporaryAppDataDir();
})
 
test.afterEach(async () => {
  await electronApp.close();
  fs.rmSync(tmpAppDataDir, { recursive: true, force: true });
})
 
test('opens repository window on first start', async () => {
  electronApp = await launchApp(tmpAppDataDir);

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

test("adds default repository if no repository is configured", async () => {
  electronApp = await launchApp(tmpAppDataDir);

  await waitForKopiaToStartup();

  const configs = await electronApp.evaluate(async ({app}) => {
    return app.testHooks.allConfigs();
  })
  expect(configs).toStrictEqual([DEFAULT_REPO_ID]);
});

test("doesn't open repository window if the default repository config exists", async () => {
  fs.writeFileSync(path.join(tmpAppDataDir, 'kopia', `${DEFAULT_REPO_ID}.config`), '');

  electronApp = await launchApp(tmpAppDataDir);

  await waitForKopiaToStartup();
  const windows = electronApp.windows();
  expect(windows).toHaveLength(0);
});

test.describe("when non-default repository config exists", () => {
  const NON_DEFAULT_REPO_ID = 'repository-42';

  test.beforeEach(async () => {
    fs.writeFileSync(path.join(tmpAppDataDir, 'kopia', `${NON_DEFAULT_REPO_ID}.config`), '');
  })

  test("doesn't open repository window if non-default repository config exists", async () => {
    electronApp = await launchApp(tmpAppDataDir);

    await waitForKopiaToStartup();
    const windows = electronApp.windows();
    expect(windows).toHaveLength(0);
  });

  test("doesn't add default repository", async () => {
    electronApp = await launchApp(tmpAppDataDir);

    await waitForKopiaToStartup();

    const configs = await electronApp.evaluate(async ({app}) => {
      return app.testHooks.allConfigs();
    })
    expect(configs).toStrictEqual([NON_DEFAULT_REPO_ID]);
  });
})
