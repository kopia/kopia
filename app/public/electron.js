const { app, BrowserWindow, Menu, Tray, ipcMain, dialog } = require('electron')
const path = require('path');
const isDev = require('electron-is-dev');
const config = require('electron-json-config');
const { autoUpdater } = require("electron-updater");
const { resourcesPath, selectByOS } = require('./utils');
const { toggleLaunchAtStartup, willLaunchAtStartup, refreshWillLaunchAtStartup } = require('./auto-launch');
const { stopServer, actuateServer, getServerAddress, getServerCertSHA256, getServerPassword } = require('./server');
const log = require("electron-log")
const firstRun = require('electron-first-run');

ipcMain.on('fetch-config', (event, arg) => {
  event.sender.send('config-updated', config.all());
})

ipcMain.on('save-config', (event, arg) => {
  console.log('saving config', arg);
  config.setBulk(arg);
  actuateServer();
  event.returnValue = true;
})

let tray = null
let configWindow = null;
let mainWindow = null;

function advancedConfiguration() {
  if (configWindow) {
    configWindow.focus();
    return;
  }

  configWindow = new BrowserWindow({
    width: 1000,
    height: 700,
    autoHideMenuBar: true,
    webPreferences: {
      nodeIntegration: true
    },
  })

  if (isDev) {
    configWindow.loadURL('http://localhost:3000');
  } else {
    configWindow.loadFile('./build/index.html');
  }

  configWindow.on('closed', function () {
    ipcMain.removeAllListeners('status-updated-event');
    ipcMain.removeAllListeners('logs-updated-event');
    // forget the reference.
    configWindow = null;
  });
}

function showMainWindow() {
  if (mainWindow) {
    mainWindow.focus();
    return;
  }

  mainWindow = new BrowserWindow({
    width: 1000,
    height: 700,
    title: 'Kopia UI Loading...',
    autoHideMenuBar: true,
  })

  mainWindow.webContents.on('did-fail-load', () => {
    log.error('failed to load');

    // schedule another attempt in 0.5s
    if (mainWindow) {
      setTimeout(() => {
        log.info('reloading');
        if (mainWindow) {
          mainWindow.loadURL(getServerAddress() + '/?ts=' + new Date().valueOf());
        }
      }, 500)
    }
  })

  mainWindow.loadURL(getServerAddress() + '/?ts=' + new Date().valueOf());

  mainWindow.on('closed', function () {
    // forget the reference.
    mainWindow = null;
  });
}

if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  app.on('second-instance', (event, commandLine, workingDirectory) => {
    // Someone tried to run a second instance, we should focus our window.
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore()
      mainWindow.focus()
    }
  })
}

app.on('will-quit', function () {
  stopServer();
});

app.on('login', (event, webContents, request, authInfo, callback) => {
  // intercept password prompts and automatically enter password that the server has printed for us.
  const p = getServerPassword();
  if (p) {
    event.preventDefault();
    console.log('automatically logging in...');
    callback('kopia', p);
  }
});

app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
  // intercept certificate errors and automatically trust the certificate the server has printed for us. 
  const expected = 'sha256/' + Buffer.from(getServerCertSHA256(), 'hex').toString('base64');
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
  if (isDev) {
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

function setMenuBar() {
  if (process.platform === 'darwin') {
    let template = []
    const name = app.getName();
    template.unshift({
      label: name,
      submenu: [
        {
          label: 'About KopiaUI',
          role: 'about'
        },
        {
          label: 'Quit',
          accelerator: 'Command+Q',
          click() { app.quit(); }
        },
      ]
    })

    // Create the Menu
    const menu = Menu.buildFromTemplate(template);
    Menu.setApplicationMenu(menu);
  }
}


function hideFromDock() {
  if (process.platform === 'darwin') {
    app.dock.hide();
  }
}

app.on('ready', () => {
  log.transports.file.level = "debug"
  autoUpdater.logger = log

  setMenuBar();
  if (maybeMoveToApplicationsFolder()) {
    return
  }

  hideFromDock();

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
  actuateServer();
  if (firstRun()) {
    showMainWindow();
  }
})

function updateTrayContextMenu() {
  console.log('updating tray...');
  const contextMenu = Menu.buildFromTemplate([
    { label: 'Show Main Window', click: showMainWindow },
    { label: 'Advanced Configuration...', click: advancedConfiguration },
    { label: 'Check For Updates Now', click: checkForUpdates },
    { type: 'separator' },
    { label: 'Launch At Startup', type: 'checkbox', click: toggleLaunchAtStartup, checked: willLaunchAtStartup() },
    { label: 'Quit', role: 'quit' },
  ])

  tray.setContextMenu(contextMenu);
}
