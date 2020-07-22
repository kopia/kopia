const { ipcMain } = require('electron');
const log = require("electron-log");

const AutoLaunch = require('auto-launch');

const autoLauncher = new AutoLaunch({
    name: 'Kopia',
    mac: {
        useLaunchAgent: true,
    },
});

let enabled = false;

module.exports = {
    willLaunchAtStartup() {
        return enabled;
    },
    toggleLaunchAtStartup() {
        if (enabled) {
            log.info('disabling autorun');
            autoLauncher.disable()
                .then(() => { enabled = false; ipcMain.emit('launch-at-startup-updated'); })
                .catch((err) => log.info(err));
        } else {
            log.info('enabling autorun');
            autoLauncher.enable()
                .then(() => { enabled = true; ipcMain.emit('launch-at-startup-updated'); })
                .catch((err) => log.info(err));
        }
    },
    refreshWillLaunchAtStartup() {
        autoLauncher.isEnabled()
            .then((isEnabled) => {
                enabled = isEnabled;
                ipcMain.emit('launch-at-startup-updated');
            })
            .catch(function (err) {
                log.info('unable to get autoLauncher state', err);
            });
    },
}