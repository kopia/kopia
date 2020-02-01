const path = require('path');
const isDev = require('electron-is-dev');

const osShortName = function() {
    switch (process.platform) {
        case "win32":
            return "win"
        case "darwin":
            return "mac"
        case "linux":
            return "linux"
        default:
            return null
    }
}();

module.exports = {
    resourcesPath: function () {
        if (isDev) {
            return path.join(__dirname, "..", "resources", osShortName);
        }
        return process.resourcesPath;
    },
    defaultServerBinary: function () {
        if (isDev) {
            return {
                "mac": path.join(__dirname, "..", "..", "dist", "kopia_darwin_amd64", "kopia"),
                "win": path.join(__dirname, "..", "..", "dist", "kopia_windows_amd64", "kopia.exe"),
                "linux": path.join(__dirname, "..", "..", "dist", "kopia_linux_amd64", "kopia"),
            }[osShortName]
        }

        return {
            "mac": path.join(process.resourcesPath, "server", "kopia"),
            "win": path.join(process.resourcesPath, "server", "kopia.exe"),
            "linux": path.join(process.resourcesPath, "server", "kopia"),
        }[osShortName]
    },
    selectByOS: function (x) {
        return x[osShortName]
    },
}