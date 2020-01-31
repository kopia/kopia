const { ipcMain } = require('electron');

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
            console.log('disabling autorun');
            autoLauncher.disable()
                .then(() => { enabled = false; ipcMain.emit('launch-at-startup-updated'); })
                .catch((err) => console.log(err));
        } else {
            console.log('enabling autorun');
            autoLauncher.enable()
                .then(() => { enabled = true; ipcMain.emit('launch-at-startup-updated'); })
                .catch((err) => console.log(err));
        }
    },
    refreshWillLaunchAtStartup() {
        autoLauncher.isEnabled()
            .then((isEnabled) => {
                enabled = isEnabled;
                ipcMain.emit('launch-at-startup-updated');
            })
            .catch(function (err) {
                console.log('unable to get autoLauncher state', err);
            });
    },
}