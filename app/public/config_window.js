const { BrowserWindow, ipcMain } = require('electron')
const isDev = require('electron-is-dev');

let configWindow = null;

forwardEventToConfigWindow('status-updated-event');
forwardEventToConfigWindow('logs-updated-event');
forwardEventToConfigWindow('config-list-updated-event');

function forwardEventToConfigWindow(event) {
    ipcMain.addListener(event, args => {
        if (configWindow) {
            console.log('forwarding', event, args);
            configWindow.webContents.send(event, args);
        }
    });
}

module.exports = {
    isConfigWindowOpen() {
        return configWindow != null
    },

    showConfigWindow() {
        if (configWindow) {
            configWindow.focus();
            return;
        }

        configWindow = new BrowserWindow({
            width: 1000,
            height: 700,
            autoHideMenuBar: true,
            webPreferences: {
                nodeIntegration: true,
            },
        })

        if (isDev) {
            configWindow.loadURL('http://localhost:3000/?ts=' + new Date().valueOf());
        } else {
            configWindow.loadFile('./build/index.html');
        }

        configWindow.on('closed', function () {
            // forget the reference.
            configWindow = null;
        });
    },
}
