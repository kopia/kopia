const fs = require('fs');
const path = require('path');
const { app, ipcMain } = require("electron");
const { hasCliConfigFlag, returnConfigFileSuffix, returnCliConfig, hasPortableFlag, hasLogDirFlag, returnCliLogDir, hasCacheDirFlag, returnCliCacheDir } = require('./cli');

let configs = {};

let configDir = "";
let isPortable = false;
let firstRun = false;

function checkForPortableConfig() {
    if (hasPortableFlag() && hasCliConfigFlag()) {
        isPortable = true;
    } else {
        globalConfigDir();
    }
    return isPortable;
}

function updateCacheDir(repoID) {
    if (firstRun) {
        return;
    }
    let configuration = JSON.parse(fs.readFileSync(configs[repoID]));
    let cacheDir = "";

    if (hasCliConfigFlag() && isPortable) {
        // put the cache directory for the repository next to its config
        // still add repoID to the path in the event there are multiple configs in one folder
        cacheDir = path.join(path.dirname(configs[repoID]), "cache", repoID);
    }
    if (hasCliConfigFlag() && !isPortable) {
        // put the cache directory in the standard non-portable location
        cacheDir = path.join(app.getPath("appData"), "kopia", "cache", repoID);
    }
    if (!hasCliConfigFlag() && isPortable) {
        // put the cache directory in the standard portable location
        cacheDir = path.join(configDir, "cache", repoID);
    }
    if (hasCacheDirFlag()) {
        // put manually specified cache directory last so it overrides above options
        cacheDir = returnCliCacheDir();
    }

    configuration.caching.cacheDirectory = path.resolve(cacheDir);
    fs.writeFileSync(configs[repoID], JSON.stringify(configuration, null, 2));

    // return the new directory for logging purposes
    return cacheDir;
}

function returnLogDir(repoID) {
    if (hasLogDirFlag()) {
        return returnCliLogDir;
    }
    if (hasCliConfigFlag() && isPortable) {
        // put the log directory for the repository next to its config
        // still add repoID to the path in the event there are multiple configs in one folder
        return path.resolve(path.join(path.dirname(configs[repoID]), "logs", repoID));
    }
    if (!hasCliConfigFlag() && isPortable) {
        // put the log directory in the standard portable location
        return path.resolve(path.join(configDir, "logs", repoID));
    }
}

// returns the list of directories to be checked for portable configurations
function portableConfigDirs() {
    let result = [];

    if (process.env.KOPIA_UI_PORTABLE_CONFIG_DIR) {
        result.push(process.env.KOPIA_UI_PORTABLE_CONFIG_DIR);
    }

    if (process.platform === "darwin") {
        // on Mac support 'repositories' directory next to the KopiaUI.app
        result.push(path.join(path.dirname(app.getPath("exe")), "..", "..", "..", "repositories"));
    } else {
        // on other platforms support 'repositories' directory next to directory
        // containing executable or 'repositories' subdirectory.
        result.push(path.join(path.dirname(app.getPath("exe")), "repositories"));
        result.push(path.join(path.dirname(app.getPath("exe")), "..", "repositories"));
    }

    return result;
}

function buildPortableLayout() {
    let dir = "";
    if (process.platform === "darwin") {
        dir = path.join(path.dirname(app.getPath("exe")), "..", "..", "..", "repositories");
        fs.mkdirSync(dir, { recursive: true, mode: parseInt('0700',8) });

    } else {
        dir = path.join(path.dirname(app.getPath("exe")), "repositories")
        fs.mkdirSync(dir, { recursive: true, mode: parseInt('0700',8) });
    }
    configDir = dir;
}

function globalConfigDir() {

    if (!configDir) {
        // try portable config dirs in order.
        portableConfigDirs().forEach(d => {
            if (configDir) {
                return;
            }
            
            d = path.normalize(d);

            if (!fs.existsSync(d)) {
                return;
            }

            configDir = d;
            isPortable = true;
        });

        if (!configDir && hasPortableFlag()) {
            buildPortableLayout();
            isPortable = true;
        }

        // still not set, fall back to per-user config dir.
        // we use the same directory that is used by Kopia CLI.
        if (!configDir) {
            configDir = path.join(app.getPath("appData"), "kopia");
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

    configs[id] = path.join(configDir, id);
    return id;
}

ipcMain.on('config-list-fetch', (event, arg) => {
    emitConfigListUpdated();
});

function emitConfigListUpdated() {
    ipcMain.emit('config-list-updated-event', allConfigs());
};

function deleteConfigIfDisconnected(repoID) {
    if (repoID === "repository") {
        // never delete default repository config
        return false;
    }

    if (!fs.existsSync(configs[repoID])) {
        delete (configs[repoID]);
        emitConfigListUpdated();
        return true;
    }

    return false;
}

function loadConfigsFromDir() {
    fs.mkdirSync(globalConfigDir(), { recursive: true, mode: parseInt('0700',8) });
    let entries = fs.readdirSync(globalConfigDir());

    entries.filter(e => path.extname(e) === returnConfigFileSuffix()).forEach(v => {
        const repoID = v.replace(returnConfigFileSuffix(), "");
        configs[repoID] = path.join(globalConfigDir(), repoID + returnConfigFileSuffix());
    });

    if (!configs["repository"]) {
        configs["repository"] = path.join(configDir, "repository.config");
        firstRun = true;
    }
}

module.exports = {

    returnConfigPath(repoID) {
        return configs[repoID];
    },

    returnLogDir(repoID) {
        return returnLogDir(repoID);
    },

    updateCacheDir(repoID) {
        return updateCacheDir(repoID);
    },

    loadConfigs() {
        if (hasCliConfigFlag()) {
            configs = returnCliConfig();
        } else {
            loadConfigsFromDir();
        }
    },

    setUserData() {
        checkForPortableConfig();
        if (isPortable) {
            if (hasCliConfigFlag()) {
              // in cli portable mode, we will put caches next to each folder, so set this to temp to avoid clutter.
              app.setPath('userData', path.join(app.getPath("temp"), "KopiaUiCache"));
            } else {
              // in standard portable mode, write cache under 'repositories'
              app.setPath('userData', path.join(configDir, 'cache'));
            }
        }
    },

    isPortableConfig() {
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

    allConfigs
}
