const fs = require('fs');
const path = require('path');
const Electron = require('electron');
const log = require("electron-log")

let configs = {};
const configFileSuffix = ".repo";

let configDir = "";
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
    if (!configDir) {
        // try portable config dirs in order.
        portableConfigDirs().forEach(d => {
            if (configDir) {
                return;
            }
            
            d = path.normalize(d)

            if (!fs.existsSync(d)) {
                console.log('portable configuration directory not found in', d);
                return;
            }

            console.log('portable configuration directory found in', d);
            configDir = d;
            isPortable = true;
        });

        // still not set, fall back to per-user config dir.
        if (!configDir) {
            configDir = path.join(Electron.app.getPath("userData"), "repositories");
            console.log('using per-user configuration directory:', configDir);
        }
    }

    return configDir;
}

function newConfigForRepo(repoID) {
    const configFile = path.join(globalConfigDir(), repoID + configFileSuffix);

    let data = {};

    try {
        const b = fs.readFileSync(configFile);
        data = JSON.parse(b);
        log.info('loaded: ' + configFile);
    } catch (e) {
        data = {
        }

        if (repoID == "default") {
            data.description = "My Repository";
        } else {
            data.description = "Unnamed Repository";
        }

        if (repoID == "default" && !isPortable) {
            data.configFile = "";
        } else {
            data.configFile = "./" + repoID + ".config";
        }

        log.info('initializing empty config: ' + configFile);
    }

    return {
        get(key) {
            return data[key];
        },

        setBulk(dict) {
            for (let k in dict) {
                data[k] = dict[k];
            }

            fs.writeFileSync(configFile, JSON.stringify(data));

            emitConfigListUpdated();
        },

        all() {
            return data;
        }
    }
};

Electron.ipcMain.on('config-get', (event, arg) => {
    const c = configs[arg.repoID];
    if (c) {
        event.returnValue = c.all();
    } else {
        event.returnValue = {};
    }
});

Electron.ipcMain.on('config-add', (event, arg) => {
    const c = newConfigForRepo(arg.repoID);
    configs[arg.repoID] = c
    emitConfigListUpdated();    
    c.setBulk(arg.data);

    event.returnValue = true;
});

function currentConfigSummary() {
    let result = [];

    for (let k in configs) {
        result.push({ repoID: k, desc: configs[k].get('description') });
    }

    return result;
}

Electron.ipcMain.on('config-list-fetch', (event, arg) => {
    emitConfigListUpdated();
});

function emitConfigListUpdated() {
    Electron.ipcMain.emit('config-list-updated-event', currentConfigSummary());
};

module.exports = {
    loadConfigs() {
        fs.mkdirSync(globalConfigDir(), { recursive: true, mode: 0700 });
        let entries = fs.readdirSync(globalConfigDir());

        let count = 0;
        entries.filter(e => path.extname(e) == configFileSuffix).forEach(v => {
            const repoID = v.replace(configFileSuffix, "");
            console.log('found config for', repoID);
            configs[repoID] = newConfigForRepo(repoID);
            count++;
        });

        if (!count) {
            firstRun = true;
            const c = newConfigForRepo('default');
            c.setBulk({});
            configs['default'] = c;
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

    currentConfigSummary,

    configForRepo(repoID) {
        let c = configs[repoID];
        if (c) {
            return c;
        }

        c = newConfigForRepo(repoID);
        configs[repoID] = c
        emitConfigListUpdated();
        return c;
    },

    deleteConfigForRepo(repoID) {
        delete (configs[repoID]);
        fs.unlinkSync(path.join(globalConfigDir(), repoID + configFileSuffix));
        emitConfigListUpdated();
    },
}