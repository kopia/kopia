const { app, BrowserWindow, Menu, Tray, ipcMain, dialog } = require('electron')
const path = require('path');
const isDev = require('electron-is-dev');
const { autoUpdater } = require("electron-updater");
const { resourcesPath, selectByOS } = require('./utils');
const { toggleLaunchAtStartup, willLaunchAtStartup, refreshWillLaunchAtStartup } = require('./auto-launch');
const { serverForRepo } = require('./server');
const log = require("electron-log")
const { loadConfigs, configForRepo, deleteConfigForRepo, currentConfigSummary, configDir, isFirstRun, isPortableConfig } = require('./config');
const { showConfigWindow, isConfigWindowOpen } = require('./config_window');

app.name = 'KopiaUI';

if (isPortableConfig()) {
  // in portable mode, write cache under 'repositories'
  app.setPath('userData', path.join(configDir(), 'cache'));
}

ipcMain.on('config-save', (event, arg) => {
  console.log('saving config', arg);
  configForRepo(arg.repoID).setBulk(arg.config);
  serverForRepo(arg.repoID).actuateServer();
  event.returnValue = true;
})

ipcMain.on('config-delete', (event, arg) => {
  console.log('deleting config', arg);
  serverForRepo(arg.repoID).stopServer();
  deleteConfigForRepo(arg.repoID);
  event.returnValue = true;
})

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
  currentConfigSummary().forEach(v => serverForRepo(v.repoID).stopServer());
});

app.on('login', (event, webContents, request, authInfo, callback) => {
  const repoID = repoIDForWebContents[webContents.id];

  // intercept password prompts and automatically enter password that the server has printed for us.
  const p = serverForRepo(repoID).getServerPassword();
  if (p) {
    event.preventDefault();
    console.log('automatically logging in...');
    callback('kopia', p);
  }
});

app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
  const repoID = repoIDForWebContents[webContents.id];
  console.log('cert error', repoID);
  // intercept certificate errors and automatically trust the certificate the server has printed for us. 
  const expected = 'sha256/' + Buffer.from(serverForRepo(repoID).getServerCertSHA256(), 'hex').toString('base64');
  if (certificate.fingerprint === expected) {
    console.log('accepting server certificate.');

    // On certificate error we disable default behaviour (stop loading the page)
    // and we then say "it is all fine - true" to the callback
    event.preventDefault();
    callback(true);
    return;
  }

  console.log('certificate error:', certificate.fingerprint, expected);
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
    if (isConfigWindowOpen() || repoWindows) {
      app.dock.show();
    } else {
      app.dock.hide();
    }
  }
}

function showAllRepoWindows() {
  currentConfigSummary().forEach(v => showRepoWindow(v.repoID));
}

app.on('ready', () => {
  loadConfigs();

  if (isPortableConfig()) {
    const logDir = path.join(configDir(), "logs");

    log.transports.file.resolvePath = (variables) => path.join(logDir, variables.fileName);
  }

  log.transports.file.level = "debug"
  autoUpdater.logger = log

  if (maybeMoveToApplicationsFolder()) {
    return
  }

  updateDockIcon();

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

  currentConfigSummary().forEach(v => serverForRepo(v.repoID).actuateServer());

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

function updateTrayContextMenu() {
  if (!tray) {
    return;
  }

  let reposTemplates = [];

  currentConfigSummary().forEach(v => {
    reposTemplates.push(
      {
        label: v.desc, click: () => showRepoWindow(v.repoID),
      },
    );
  });

  reposTemplates.sort((a, b) => a.label.localeCompare(b.label));

  template = reposTemplates.concat([
    { type: 'separator' },
    { label: 'Configure Repositories...', click: () => showConfigWindow() },
    { type: 'separator' },
    { label: 'Check For Updates Now', click: checkForUpdates },
    { type: 'separator' },
    { label: 'Launch At Startup', type: 'checkbox', click: toggleLaunchAtStartup, checked: willLaunchAtStartup() },
    { label: 'Quit', role: 'quit' },
  ]);

  tray.setContextMenu(Menu.buildFromTemplate(template));
}
