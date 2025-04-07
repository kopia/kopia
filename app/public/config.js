const fs = await import('fs');
const path = await import('path');
const Electron = await import('electron');
const log = await import("electron-log");

let configs = {};
const configFileSuffix = ".config";

let myConfigDir = "";
let isPortable = false;
let firstRun = false;

// returns the list of directories to be checked for portable configurations
function portableConfigDirs() {
    let result = [];

    if (process.env.KOPIA_UI_PORTABLE_CONFIG_DIR) {
        result.push(process.env.KOPIA_UI_PORTABLE_CONFIG_DIR);
    }

    if (process.platform == "darwin") {
        // on Mac support 'repositories' directory next to the KopiaUI.app
        result.push(path.join(path.dirname(Electron.app.getPath("exe")), "..", "..", "..", "repositories"));
    } else {
        // on other platforms support 'repositories' directory next to directory
        // containing executable or 'repositories' subdirectory.
        result.push(path.join(path.dirname(Electron.app.getPath("exe")), "repositories"));
        result.push(path.join(path.dirname(Electron.app.getPath("exe")), "..", "repositories"));
    }

    return result;
}

function globalConfigDir() {
    if (!myConfigDir) {
        // try portable config dirs in order.
        portableConfigDirs().forEach(d => {
            if (myConfigDir) {
                return;
            }

            d = path.normalize(d)

            if (!fs.existsSync(d)) {
                return;
            }

            myConfigDir = d;
            isPortable = true;
        });

        // still not set, fall back to per-user config dir.
        // we use the same directory that is used by Kopia CLI.
        if (!myConfigDir) {
            myConfigDir = path.join(Electron.app.getPath("appData"), "kopia");
        }
    }

    return myConfigDir;
}

export function allConfigs() {
    let result = [];

    for (let k in configs) {
        result.push(k);
    }

    return result;
}

export function addNewConfig() {
    let id;

    if (!configs) {
        // first repository is always named "repository" to match Kopia CLI.
        id = "repository";
    } else {
        id = "repository-" + new Date().valueOf();
    }

    configs[id] = true;
    return id;
}

Electron.ipcMain.on('config-list-fetch', (event, arg) => {
    emitConfigListUpdated();
});

function emitConfigListUpdated() {
    Electron.ipcMain.emit('config-list-updated-event', allConfigs());
};

export function deleteConfigIfDisconnected(repoID) {
    if (repoID === "repository") {
        // never delete default repository config
        return false;
    }

    if (!fs.existsSync(path.join(globalConfigDir(), repoID + configFileSuffix))) {
        delete (configs[repoID]);
        emitConfigListUpdated();
        return true;
    }

    return false;
}

export function loadConfigs() {
    fs.mkdirSync(globalConfigDir(), { recursive: true, mode: 0o700 });
    let entries = fs.readdirSync(globalConfigDir());

    let count = 0;
    entries.filter(e => path.extname(e) == configFileSuffix).forEach(v => {
        const repoID = v.replace(configFileSuffix, "");
        configs[repoID] = true;
        count++;
    });

    if (!configs["repository"]) {
        configs["repository"] = true;
        firstRun = true;
    }
};


export function isPortableConfig() {
    globalConfigDir();
    return isPortable;
};

export function isFirstRun() {
    return firstRun;
}

export function configDir() {
    return globalConfigDir();
}

export function configForRepo(repoID) {
    let c = configs[repoID];
    if (c) {
        return c;
    }

    configs[repoID] = true;
    emitConfigListUpdated();
    return c;
}
