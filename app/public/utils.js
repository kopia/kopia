import { app } from 'electron';
import path from 'path';
const __dirname = import.meta.dirname;

const osShortName = function () {
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

export function iconsPath() {
    if (!app.isPackaged) {
        return path.join(__dirname, "..", "resources", osShortName, "icons");
    }

    return path.join(process.resourcesPath, "icons");
}

export function publicPath() {
    if (!app.isPackaged) {
        return path.join(__dirname, "..", "public");
    }

    return process.resourcesPath;
}

export function defaultServerBinary() {
    if (!app.isPackaged) {
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
}
export function selectByOS(x) {
    return x[osShortName]
}
