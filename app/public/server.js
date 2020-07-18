const { ipcMain } = require('electron');
const path = require('path');

const { defaultServerBinary } = require('./utils');
const { spawn } = require('child_process');
const log = require("electron-log")
const { configForRepo, configDir, isPortableConfig } = require('./config');

let servers = {};

function newServerForRepo(repoID) {
    const config = configForRepo(repoID);

    let runningServerProcess = null;
    let runningServerCertSHA256 = "";
    let runningServerPassword = "";
    let runningServerAddress = "";
    let serverLog = [];

    const maxLogLines = 100;

    return {
        actuateServer() {
            log.info('actuating Server', repoID);
            this.stopServer();
            if (!config.get('remoteServer')) {
                this.startServer();
            }
        },

        startServer() {
            let kopiaPath = config.get('kopiaPath');
            if (!kopiaPath) {
                kopiaPath = defaultServerBinary();
            }

            let args = [];

            args.push('server', '--ui');

            let configFile = config.get('configFile');
            if (configFile) {
                args.push("--config-file", path.resolve(configDir(), configFile));
                if (isPortableConfig) {
                    const cacheDir = path.resolve(configDir(), "cache", repoID);
                    const logsDir = path.resolve(configDir(), "logs", repoID);
                    args.push("--cache-directory", cacheDir);
                    args.push("--log-dir", logsDir);
                }
            }

            args.push('--random-password')
            args.push('--tls-generate-cert')
            args.push('--address=localhost:0')
            // args.push('--auto-shutdown=600s')

            log.info(`spawning ${kopiaPath} ${args.join(' ')}`);
            let childEnv = {
                // set environment variable that causes given prefix to be returned in the HTML <title>
                KOPIA_UI_TITLE_PREFIX: "[" + config.get('description') + "] ",
                ...process.env
            }
            runningServerProcess = spawn(kopiaPath, args, {
                env: childEnv,
            });
            this.raiseStatusUpdatedEvent();

            runningServerProcess.stdout.on('data', this.appendToLog.bind(this));
            runningServerProcess.stderr.on('data', this.detectServerParam.bind(this));

            const p = runningServerProcess;
            
            runningServerProcess.on('close', (code, signal) => {
                this.appendToLog(`child process exited with code ${code} and signal ${signal}`);
                if (runningServerProcess === p) {
                    runningServerAddress = "";
                    runningServerPassword = "";
                    runningServerCertSHA256 = "";
                    runningServerProcess = null;
                    this.raiseStatusUpdatedEvent();
                }
            });
        },

        detectServerParam(data) {
            let lines = (data + '').split('\n');
            for (let i = 0; i < lines.length; i++) {
                const p = lines[i].indexOf(": ");
                if (p < 0) {
                    continue
                }

                const key = lines[i].substring(0, p);
                const value = lines[i].substring(p + 2);
                switch (key) {
                    case "SERVER PASSWORD":
                        runningServerPassword = value;
                        this.raiseStatusUpdatedEvent();
                        break;

                    case "SERVER CERT SHA256":
                        runningServerCertSHA256 = value;
                        this.raiseStatusUpdatedEvent();
                        break;

                    case "SERVER ADDRESS":
                        runningServerAddress = value;
                        this.raiseStatusUpdatedEvent();
                        break;
                }
            }

            this.appendToLog(data);
        },

        appendToLog(data) {
            const l = serverLog.push(data);
            if (l > maxLogLines) {
                serverLog.splice(0, 1);
            }

            ipcMain.emit('logs-updated-event', {
                repoID: repoID,
                logs: serverLog.join(''),
            });
            log.info(`${data}`);
        },

        stopServer() {
            if (!runningServerProcess) {
                log.info('stopServer: server not started');
                return;
            }

            runningServerProcess.kill();
            runningServerAddress = "";
            runningServerPassword = "";
            runningServerCertSHA256 = "";
            runningServerProcess = null;
            this.raiseStatusUpdatedEvent();
        },

        getServerAddress() {
            if (config.get('remoteServer')) {
                return config.get('remoteServerAddress');
            } else {
                return runningServerAddress;
            }
        },

        getServerCertSHA256() {
            if (config.get('remoteServer')) {
                return config.get('remoteServerCertificateSHA256');
            } else {
                return runningServerCertSHA256;
            }
        },

        getServerPassword() {
            if (config.get('remoteServer')) {
                return config.get('remoteServerPassword');
            } else {
                return runningServerPassword;
            }
        },

        getServerStatus() {
            if (config.get('remoteServer')) {
                return "Remote";
            } else {
                if (!runningServerProcess) {
                    return "Stopped";
                }

                if (runningServerCertSHA256 && runningServerAddress && runningServerPassword) {
                    return "Running";
                }

                return "Starting";
            }
        },

        raiseStatusUpdatedEvent() {
            const args = {
                repoID: repoID,
                status: this.getServerStatus(),
                serverAddress: this.getServerAddress() || "<pending>",
                serverCertSHA256: this.getServerCertSHA256() || "<pending>",
            };

            ipcMain.emit('status-updated-event', args);
        },
    };
};

ipcMain.on('status-fetch', (event, args) => {
    const repoID = args.repoID;
    const s = servers[repoID]
    if (s) {
        s.raiseStatusUpdatedEvent();
    }
})

module.exports = {
    serverForRepo(repoID) {
        let s = servers[repoID];
        if (s) {
            return s;
        }

        s = newServerForRepo(repoID);
        servers[repoID] = s;
        return s;
    }
}
