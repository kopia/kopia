const log = require("electron-log")
const fs = require('fs');
const path = require('path');

const { app } = require("electron")

const cliArgs = process.argv
const configFileSuffix = ".config";

let hasCliConfigFlag = false;
let hasPortableFlag = false;
let hasLogDirFlag = false;
let hasCacheDirFlag = false;

function invalidConfigFile(file) {
    log.error(`invalid configuration file passed on command line: ${file}`);
    console.log(`Error: Invalid configuration file passed on command line.\
                 File Name: ${file}\
                 Valid configuration files end in ${configFileSuffix}.\
                 Exiting...`);
    app.exit(1);
}

function configFileNotFound(file) {
    log.error(`unable to locate configuration file: ${file}`);
    console.log(`Error: Unable to verify existance of configuration file.\
                 File Name: ${file}\
                 Please check that the path is correct and try again.\
                 Exiting...`);
    app.exit(1);
}

// this isn't pretty but it doesn't require any additional dependencies.
function outputHelp() {
    console.log(`usage: ${app.getPath('exe')} [<flags>]`);
    console.log(``);
    console.log(`KopiaUI - Fast And Secure Open-Source Backup`);
    console.log(``);
    console.log(`Flags:`);
    console.log(`    --help                    Shows this help message.`);
    console.log(`    --portable                Runs the UI in portable mode.`)
    console.log(`    --config-file="<file>"    Specify the configuration file used.`);
    console.log(`    --log-dir="<dir>"         Specify the directory where logs should be stored.`)
    console.log(`    --cache-directory="<dir>  Specify the directory where cache files should be stored.`);
    console.log(``);
    console.log(`Usage Notes:`);
    console.log(`    You can call each flag multiple times without causing an error.`);
    console.log(`    However, only --config-file will have each file read and opened in the UI.`);
    console.log(`    For cache-directory and log-dir, only the first invokation will be used.`);
    console.log(``);
    console.log(`    log-dir will place repository logs into the provided directory regardless`);
    console.log(`    of additional flags.`);
    console.log(``);
    console.log(`    Because --cache-directory rewrites part of the repository configuration, it is only`);
    console.log(`    used when either --config-file or --portable is called.`)
    console.log(``);
    console.log(`    Calling --portable without additional options will launch the UI in portable mode,`);
    console.log(`    and create the required directory structure.`);
    console.log(``);
    console.log(`    Calling --portable with --config-file will place the repository's cache and log`);
    console.log(`    files in the same directory as the configuration file.`);
    console.log(``);
    console.log(`    Calling --config-file without --portable will use the standard Kopia directory`);
    console.log(`    locations.`);
    console.log(``);
    console.log(`    Calling --config-file will put the UI into a manual configuration mode, and you`);
    console.log(`    will not be able to connect to new repositories. Please use the kopia command`);
    console.log(`    to create your repository configurations via the following layout:`);
    console.log(`        kopia --config-file=<path>/<file>.config repository connect <args>`);
}

module.exports = {

    hasPortableFlag() {
        return hasPortableFlag;
    },

    hasCliConfigFlag() {
        return hasCliConfigFlag;
    },

    hasLogDirFlag() {
        return hasLogDirFlag;
    },

    hasCacheDirFlag() {
        return hasCacheDirFlag;
    },

    parseCliFlags() {
        if (cliArgs.some(f => f.includes("--config-file"))) {
            hasCliConfigFlag = true;
        }
        if (cliArgs.some(f => f.includes("--portable"))) {
            hasPortableFlag = true;
        }
        if (cliArgs.some(f => f.includes("--log-dir"))) {
            hasLogDirFlag = true;
        }
        if (cliArgs.some(f => f.includes("--cache-directory"))) {
            hasCacheDirFlag = true;
        }
        if (cliArgs.some(f => f.includes("--help"))) {
            outputHelp();
            app.exit(0);
        }
    },

    returnCliConfig() {
        let result = {}

        cliArgs.forEach(i => {
            if (i.includes("--config-file")) {
                let file = i.replace("--config-file=", "");
                if (!file.includes(configFileSuffix)) {
                    invalidConfigFile(file);
                }
                if (!fs.existsSync(file)) {
                    configFileNotFound(file);
                }
                let repoID = path.basename(file, configFileSuffix);
                result[repoID] = path.resolve(file);
            }
        })

        return result;
    },

    returnCliLogDir() {
        cliArgs.forEach(i => {
            if (i.includes("--log-dir=")) {
                return i.replace("--log-dir=", "").replace('"', "");
            }
        })
    },

    returnCliCacheDir() {
        cliArgs.forEach(i => {
            if (i.includes("--cache-directory=")) {
                return i.replace("--cache-directory=", "").replace('"', "");
            }
        })
    },

    returnConfigFileSuffix() {
        return configFileSuffix;
    }

}
