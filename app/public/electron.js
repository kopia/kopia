const { app, BrowserWindow, Notification, Menu, Tray, ipcMain, dialog, shell } = require('electron')
const path = require('path');
const isDev = require('electron-is-dev');
const { autoUpdater } = require("electron-updater");
const { resourcesPath, selectByOS } = require('./utils');
const { toggleLaunchAtStartup, willLaunchAtStartup, refreshWillLaunchAtStartup } = require('./auto-launch');
const { serverForRepo } = require('./server');
const log = require("electron-log");
const { loadConfigs, allConfigs, deleteConfigIfDisconnected, addNewConfig, configDir, isFirstRun, isPortableConfig } = require('./config');

app.name = 'KopiaUI';

if (isPortableConfig()) {
  // in portable mode, write cache under 'repositories'
  app.setPath('userData', path.join(configDir(), 'cache'));
}

let tray = null
let repoWindows = {};
let repoIDForWebContents = {};

function showRepoWindow(repoID) {
  if (repoWindows[repoID]) {
    repoWindows[repoID].focus();
    return;
  }

  let rw = new BrowserWindow({
    width: 1000,
    height: 700,
    title: 'KopiaUI is Loading...',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(resourcesPath(), 'preload.js'),
    },
  })

  repoWindows[repoID] = rw

  const wcID = rw.webContents.id;
  repoIDForWebContents[wcID] = repoID

  rw.webContents.on('did-fail-load', () => {
    log.error('failed to load');

    // schedule another attempt in 0.5s
    if (repoWindows[repoID]) {
      setTimeout(() => {
        log.info('reloading');
        if (repoWindows[repoID]) {
          repoWindows[repoID].loadURL(serverForRepo(repoID).getServerAddress() + '/?ts=' + new Date().valueOf());
        }
      }, 500)
    }
  })

  rw.loadURL(serverForRepo(repoID).getServerAddress() + '/?ts=' + new Date().valueOf());
  updateDockIcon();

  rw.on('closed', function () {
    // forget the reference.
    rw = null;
    delete (repoWindows[repoID]);
    delete (repoIDForWebContents[wcID]);

    const s = serverForRepo(repoID);
    if (deleteConfigIfDisconnected(repoID)) {
      s.stopServer();
    }

    updateDockIcon();
  });
}

if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  app.on('second-instance', (event, commandLine, workingDirectory) => {
    // Someone tried to run a second instance, we should focus our window.
    for (let repoID in repoWindows) {
      let rw = repoWindows[repoID];
      if (rw.isMinimized()) {
        rw.restore()
      }

      rw.focus()
    }
  })
}

app.on('will-quit', function () {
  allConfigs().forEach(v => serverForRepo(v).stopServer());
});

app.on('login', (event, webContents, request, authInfo, callback) => {
  const repoID = repoIDForWebContents[webContents.id];

  // intercept password prompts and automatically enter password that the server has printed for us.
  const p = serverForRepo(repoID).getServerPassword();
  if (p) {
    event.preventDefault();
    log.info('automatically logging in...');
    callback('kopia', p);
  }
});

app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
  const repoID = repoIDForWebContents[webContents.id];
  // intercept certificate errors and automatically trust the certificate the server has printed for us.
  const expected = 'sha256/' + Buffer.from(serverForRepo(repoID).getServerCertSHA256(), 'hex').toString('base64');
  if (certificate.fingerprint === expected) {
    log.debug('accepting server certificate.');

    // On certificate error we disable default behaviour (stop loading the page)
    // and we then say "it is all fine - true" to the callback
    event.preventDefault();
    callback(true);
    return;
  }

  log.warn('certificate error:', certificate.fingerprint, expected);
});

// Ignore
app.on('window-all-closed', function () { })

ipcMain.handle('select-dir', async (event, arg) => {
  const result = await dialog.showOpenDialog({
    properties: ['openDirectory']
  });

  if (result.filePaths) {
    return result.filePaths[0];
  } else {
    return null;
  };
})

ipcMain.on('server-status-updated', updateTrayContextMenu);
ipcMain.on('launch-at-startup-updated', updateTrayContextMenu);

let updateAvailableInfo = null;
let updateDownloadStatusInfo = "";
let updateFailed = false;
let checkForUpdatesTriggeredFromUI = false;

// set this environment variable when developing
// to allow offering downgrade to the latest released version.
autoUpdater.allowDowngrade = process.env["KOPIA_UI_ALLOW_DOWNGRADE"] == "1";

// we will be manually triggering download and quit&install.
autoUpdater.autoDownload = false;
autoUpdater.autoInstallOnAppQuit = false;

let lastNotifiedVersion = "";

autoUpdater.on('update-available', a => {
  log.info('update available ' + a.version);

  updateAvailableInfo = a;
  updateDownloadStatusInfo = "";
  updateTrayContextMenu();

  // do not notify more than once for a particular version.
  if (checkForUpdatesTriggeredFromUI) {
    dialog.showMessageBox({buttons:["Yes", "No"], message: "An updated KopiaUI v" + a.version + " is available.\n\nDo you want to install it now?"}).then(r => {
      if (r.response == 0) {
        installUpdate();
      }
    });
    checkForUpdatesTriggeredFromUI = false;
  }

  if (lastNotifiedVersion != a.version) {
    lastNotifiedVersion = a.version;

    const n = new Notification({
      title: "New version of KopiaUI",
      body: "Version v" + a.version + " is available.\n\nClick here to download and install it.",
    });

    n.on('click', () => installUpdate());
    n.show();
  }
})

autoUpdater.on('update-not-available', () => {
  updateAvailableInfo = null;
  updateDownloadStatusInfo = "";
  updateFailed = false;
  updateTrayContextMenu();
  if (checkForUpdatesTriggeredFromUI) {
    dialog.showMessageBox({buttons:["OK"], message: "No updates available."});
    checkForUpdatesTriggeredFromUI = false;
  }
})

autoUpdater.on('download-progress', progress => {
  if (updateAvailableInfo) {
    updateDownloadStatusInfo = "Downloading Update: v" + updateAvailableInfo.version + " (" + (Math.round(progress.percent * 10) / 10.0) + "%) ...";
    updateTrayContextMenu();
  }
});

autoUpdater.on('update-downloaded', info => {
  updateDownloadStatusInfo = "Installing Update: v" + updateAvailableInfo.version + " ...";
  updateTrayContextMenu();

  setTimeout(() => {
    try {
      autoUpdater.quitAndInstall();
    } catch (e) {
      log.info('update error', e);
    }

    updateDownloadStatusInfo = null;
    updateFailed = true;
    updateTrayContextMenu();
  }, 500);
});

autoUpdater.on('error', a => {
  updateAvailableInfo = null;
  updateError = true;
  updateDownloadStatusInfo = "Error checking for updates.";
  log.info('error checking for updates', a);
  updateTrayContextMenu();
  if (checkForUpdatesTriggeredFromUI) {
    dialog.showErrorBox("Error checking for updates.", "There was an error checking for updates, try again later.");
    checkForUpdatesTriggeredFromUI = false;
  }
});

function checkForUpdates() {
  updateDownloadStatusInfo = "Checking for update...";
  updateAvailableInfo = null;
  updateTrayContextMenu();

  autoUpdater.checkForUpdates();
}

function checkForUpdatesNow() {
  checkForUpdatesTriggeredFromUI = true;
  checkForUpdates();
}

function installUpdate() {
  updateDownloadStatusInfo = "Downloading and installing update...";
  autoUpdater.downloadUpdate();
}

function viewReleaseNotes() {
  const ver = updateAvailableInfo.version + "";
  if (ver.match(/^\d{8}\./)) {
    // kopia-test builds are named yyyymmdd.0.hhmmss
    shell.openExternal("https://github.com/kopia/kopia-test-builds/releases/v" + ver);
  } else {
    shell.openExternal("https://github.com/kopia/kopia/releases/v" + ver);
  }
}

function isOutsideOfApplicationsFolderOnMac() {
  if (isDev || isPortableConfig()) {
    return false;
  }

  // this method is only available on Mac.
  if (!app.isInApplicationsFolder) {
    return false;
  }

  return !app.isInApplicationsFolder();
}

function maybeMoveToApplicationsFolder() {
  if (process.env["KOPIA_UI_TESTING"]) {
    return;
  }

  dialog.showMessageBox({
    buttons: ["Yes", "No"],
    message: "For best experience, Kopia needs to be installed in Applications folder.\n\nDo you want to move it now?"
  }).then(r => {
    if (r.response == 0) {
      app.moveToApplicationsFolder();
    } else {
      checkForUpdates();
    }
  }).catch(e => {
    log.info(e);
  });
}

function updateDockIcon() {
  if (process.platform === 'darwin') {
    let any = false
    for (const k in repoWindows) {
      any = true;
    }
    if (any) {
      app.dock.show();
    } else {
      app.dock.hide();
    }
  }
}

function showAllRepoWindows() {
  allConfigs().forEach(showRepoWindow);
}

function safeTrayHandler(ev, h) {
  tray.on(ev, () => {
    try {
      h();
    } catch (e) {
    }
  })
}

app.on('ready', () => {
  loadConfigs();

  if (isPortableConfig()) {
    const logDir = path.join(configDir(), "logs");

    log.transports.file.resolvePath = (variables) => path.join(logDir, variables.fileName);
  }

  log.transports.console.level = "warn"
  log.transports.file.level = "debug"
  autoUpdater.logger = log

  // re-check for updates every 24 hours
  setInterval(checkForUpdates, 86400000);

  tray = new Tray(
    path.join(
      resourcesPath(), 'icons',
      selectByOS({ mac: 'kopia-tray.png', win: 'kopia-tray.ico', linux: 'kopia-tray.png' })));

  tray.setToolTip('Kopia');

  // hooks exposed to tests
  if (process.env["KOPIA_UI_TESTING"]) {
    app.testHooks = {
      tray: tray,
      showRepoWindow: showRepoWindow,
    }
  }

  safeTrayHandler("click", () => tray.popUpContextMenu());
  safeTrayHandler("right-click", () => tray.popUpContextMenu());
  safeTrayHandler("double-click", () => showAllRepoWindows());

  updateTrayContextMenu();
  refreshWillLaunchAtStartup();
  updateDockIcon();

  allConfigs().forEach(repoID => serverForRepo(repoID).actuateServer());

  if (isFirstRun()) {
    // open all repo windows on first run.
    showAllRepoWindows();

    // on Windows, also show the notification.
    if (process.platform === "win32") {
      tray.displayBalloon({
        title: "Kopia is running in the background",
        content: "Click on the system tray icon to open the menu",
      });
    }
  }

  if (isOutsideOfApplicationsFolderOnMac()) {
    setTimeout(maybeMoveToApplicationsFolder, 1000);
  } else {
    checkForUpdates();
  }
})

ipcMain.addListener('config-list-updated-event', () => updateTrayContextMenu());
ipcMain.addListener('status-updated-event', () => updateTrayContextMenu());

function addAnotherRepository() {
  const repoID = addNewConfig();
  serverForRepo(repoID).actuateServer();
  showRepoWindow(repoID);
}

function updateTrayContextMenu() {
  if (!tray) {
    return;
  }

  let defaultReposTemplates = [];
  let additionalReposTemplates = [];

  allConfigs().forEach(repoID => {
    const sd = serverForRepo(repoID).getServerStatusDetails();
    let desc = "";

    if (sd.startingUp) {
      desc = "<starting up>";
    } else if (!sd.connected) {
      if (sd.initTaskID) {
        desc = "<initializing>";
      } else {
        desc = "<not connected>";
      }
    } else {
      desc = sd.description;
    }

    // put primary repository first.
    const collection = repoID === ("repository") ? defaultReposTemplates : additionalReposTemplates

    collection.push(
      {
        label: desc,
        click: () => showRepoWindow(repoID),
        toolTip: desc + " (" + repoID + ")",
      },
    );
  });

  if (additionalReposTemplates.length > 0) {
    additionalReposTemplates.sort((a, b) => a.label.localeCompare(b.label));
  }

  let autoUpdateMenuItems = [];

  if (updateDownloadStatusInfo) {
    autoUpdateMenuItems.push({ label: updateDownloadStatusInfo, enabled: false});
  } else if (updateAvailableInfo) {
    if (updateFailed) {
      autoUpdateMenuItems.push({ label: 'Update Failed, click to manually download and install v' + updateAvailableInfo.version, click: viewReleaseNotes });
    } else {
      autoUpdateMenuItems.push({ label: 'Update Available: v' + updateAvailableInfo.version, click: viewReleaseNotes });
      autoUpdateMenuItems.push({ label: 'Download And Install...', click: installUpdate });
    }
  } else {
    autoUpdateMenuItems.push({ label: "KopiaUI is up-to-date: " + app.getVersion(), enabled: false });
  }

  template = defaultReposTemplates.concat(additionalReposTemplates).concat([
    { type: 'separator' },
    { label: 'Connect To Another Repository...', click: addAnotherRepository },
    { type: 'separator' },
    { label: 'Check For Updates Now', click: checkForUpdatesNow },
  ]).concat(autoUpdateMenuItems).concat([
    { type: 'separator' },
    { label: 'Launch At Startup', type: 'checkbox', click: toggleLaunchAtStartup, checked: willLaunchAtStartup() },
    { label: 'Quit', role: 'quit' },
  ]);

  tray.setContextMenu(Menu.buildFromTemplate(template));
}
