const { app, BrowserWindow, Menu, Tray, ipcMain } = require('electron')
const path = require('path');
const isDev = require('electron-is-dev');
const config = require('electron-json-config');

const { resourcesPath, selectByOS } = require('./utils');
const { toggleLaunchAtStartup, willLaunchAtStartup, refreshWillLaunchAtStartup } = require('./auto-launch');
const { stopServer, actuateServer } = require('./server');


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
    return;
  }

  mainWindow = new BrowserWindow({
    width: 1000,
    height: 700,
    autoHideMenuBar: true,
  })

  mainWindow.loadURL('http://localhost:51515/?ts=' + new Date().valueOf());

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

// Ignore
app.on('window-all-closed', function () {})

ipcMain.on('server-status-updated', updateTrayContextMenu);
ipcMain.on('launch-at-startup-updated', updateTrayContextMenu);

app.on('ready', () => {
  tray = new Tray(
    path.join(
      resourcesPath(), 'icons',
      selectByOS({ mac: 'kopia-tray.png', win: 'kopia-tray.ico', linux: 'kopia-tray.png' })));

  tray.setToolTip('Kopia');
  updateTrayContextMenu();
  refreshWillLaunchAtStartup();
  actuateServer();
})

function updateTrayContextMenu() {
  console.log('updating tray...');
  const contextMenu = Menu.buildFromTemplate([
    { label: 'Show Main Window', click: showMainWindow },
    { label: 'Advanced Configuration...', click: advancedConfiguration },
    { type: 'separator' },
    { label: 'Launch At Startup', type: 'checkbox', click: toggleLaunchAtStartup, checked: willLaunchAtStartup() },
    { label: 'Quit', role: 'quit' },
  ])

  tray.setContextMenu(contextMenu);
}
