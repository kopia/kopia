const { app, BrowserWindow, Menu, Tray, ipcMain, dialog } = require('electron')
const path = require('path');
const isDev = require('electron-is-dev');
const { autoUpdater } = require("electron-updater");
const { resourcesPath, selectByOS } = require('./utils');
const { toggleLaunchAtStartup, willLaunchAtStartup, refreshWillLaunchAtStartup } = require('./auto-launch');
const { serverForRepo } = require('./server');
const log = require("electron-log")
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
    title: 'Kopia UI Loading...',
    autoHideMenuBar: true,
    webPreferences: {
      nodeIntegration: true,
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

ipcMain.on('server-status-updated', updateTrayContextMenu);
ipcMain.on('launch-at-startup-updated', updateTrayContextMenu);

function checkForUpdates() {
  autoUpdater.checkForUpdatesAndNotify();
}

function maybeMoveToApplicationsFolder() {
  if (isDev || isPortableConfig()) {
    return;
  }

  try {
    if (!app.isInApplicationsFolder()) {
      let result = dialog.showMessageBoxSync({
        buttons: ["Yes", "No"],
        message: "For best experience, Kopia needs to be installed in Applications folder.\n\nDo you want to move it now?"
      });
      if (result == 0) {
        return app.moveToApplicationsFolder();
      }
    }
  }
  catch (e) {
    log.error('error' + e);
    // ignore
  }
  return false;
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

app.on('ready', () => {
  loadConfigs();

  if (isPortableConfig()) {
    const logDir = path.join(configDir(), "logs");

    log.transports.file.resolvePath = (variables) => path.join(logDir, variables.fileName);
  }

  log.transports.console.level = "warn"
  log.transports.file.level = "debug"
  autoUpdater.logger = log

  if (maybeMoveToApplicationsFolder()) {
    return
  }

  checkForUpdates();

  // re-check for updates every 24 hours
  setInterval(checkForUpdates, 86400000);

  tray = new Tray(
    path.join(
      resourcesPath(), 'icons',
      selectByOS({ mac: 'kopia-tray.png', win: 'kopia-tray.ico', linux: 'kopia-tray.png' })));

  tray.setToolTip('Kopia');
  updateTrayContextMenu();
  refreshWillLaunchAtStartup();
  updateDockIcon();

  allConfigs().forEach(repoID => serverForRepo(repoID).actuateServer());

  tray.on('balloon-click', tray.popUpContextMenu);
  tray.on('click', tray.popUpContextMenu);
  tray.on('double-click', showAllRepoWindows);

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

  let reposTemplates = [];

  allConfigs().forEach(repoID => {
    const sd = serverForRepo(repoID).getServerStatusDetails();
    let desc = "";

    if (sd.connecting) {
      desc = "<starting up>";
    } else if (!sd.connected) {
      desc = "<not connected>";
    } else {
      desc = sd.description;
    }

    reposTemplates.push(
      {
        label: desc, click: () => showRepoWindow(repoID),
      },
    );
  });

  reposTemplates.sort((a, b) => a.label.localeCompare(b.label));

  template = reposTemplates.concat([
    { type: 'separator' },
    { label: 'Connect To Another Repository...', click: addAnotherRepository },
    { type: 'separator' },
    { label: 'Check For Updates Now', click: checkForUpdates },
    { type: 'separator' },
    { label: 'Launch At Startup', type: 'checkbox', click: toggleLaunchAtStartup, checked: willLaunchAtStartup() },
    { label: 'Quit', role: 'quit' },
  ]);

  tray.setContextMenu(Menu.buildFromTemplate(template));
}
