const fs = require('fs');
const path = require('path');
const Electron = require('electron');
const log = require("electron-log");

let configs = {};
const configFileSuffix = ".config";

let configDir = "";
let isPortable = false;
let firstRun = false;

// returns additional server args for starting Kopia server
function additionalServerArgs(repoID) {
    const argsFile=path.resolve(globalConfigDir(), `server_args-${repoID}.conf`)
    log.info(`Checking for additional server args in ${argsFile}`)
    if(fs.existsSync(argsFile)){
        const additionalArgs=fs.readFileSync(argsFile).toString().split("\n").filter(v => v.trim().length >= 0)
        log.info(`Found additional server args for ${repoID}: ${JSON.stringify(additionalArgs)}`)
        return additionalArgs
    } else {
        return []
    }
}

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
    if (!configDir) {
        // try portable config dirs in order.
        portableConfigDirs().forEach(d => {
            if (configDir) {
                return;
            }
            
            d = path.normalize(d)

            if (!fs.existsSync(d)) {
                return;
            }

            configDir = d;
            isPortable = true;
        });

        // still not set, fall back to per-user config dir.
        // we use the same directory that is used by Kopia CLI.
        if (!configDir) {
            configDir = path.join(Electron.app.getPath("appData"), "kopia");
        }
    }

    return configDir;
}

function allConfigs() {
    let result = [];

    for (let k in configs) {
        result.push(k);
    }

    return result;
}

function addNewConfig() {
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

function deleteConfigIfDisconnected(repoID) {
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

module.exports = {
    additionalServerArgs,
    loadConfigs() {
        fs.mkdirSync(globalConfigDir(), { recursive: true, mode: 0700 });
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
    },

    isPortableConfig() {
        globalConfigDir();
        return isPortable;
    },

    isFirstRun() {
        return firstRun;
    },

    configDir() {
        return globalConfigDir();
    },

    deleteConfigIfDisconnected,

    addNewConfig,

    allConfigs,

    configForRepo(repoID) {
        let c = configs[repoID];
        if (c) {
            return c;
        }

        configs[repoID] = true;
        emitConfigListUpdated();
        return c;
    }
}