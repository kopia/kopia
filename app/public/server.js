const { ipcMain } = require('electron');
const https = require('https');

const { defaultServerBinary } = require('./utils');
const { spawn } = require('child_process');
const log = require("electron-log")
const { isPortableConfig, returnLogDir, updateCacheDir, returnConfigPath } = require('./config');
const { hasCliConfigFlag, hasLogDirFlag } = require('./cli');

let servers = {};

function newServerForRepo(repoID) {
    let runningServerProcess = null;
    let runningServerCertSHA256 = "";
    let runningServerPassword = "";
    let runningServerAddress = "";
    let runningServerCertificate = "";
    let runningServerStatusDetails = {
        connecting: true,
    };
    let serverLog = [];

    const maxLogLines = 100;

    return {
        actuateServer() {
            log.info('actuating Server', repoID);
            this.stopServer();
            this.startServer();
        },

        startServer() {
            let kopiaPath = defaultServerBinary();
            let args = [];

            args.push('server', '--ui',
                '--tls-print-server-cert',
                '--tls-generate-cert-name=localhost',
                '--random-password',
                '--tls-generate-cert',
                '--shutdown-on-stdin', // shutdown the server when parent dies
                '--address=localhost:0');
    
            args.push(`--config-file=${returnConfigPath(repoID)}`)
 
            // the config file will need to have its cache directory updated either way
            if (isPortableConfig() || hasCliConfigFlag()) {
                let cacheDir = updateCacheDir(repoID);
                log.info(`cache directory for ${repoID} set to ${cacheDir}`);
            }

            // only provide a custom log-dir if running in some form of portable mode
            if (isPortableConfig() || hasLogDirFlag()) {
                let logDir = returnLogDir(repoID);
                log.info(`log directory for ${repoID} set to ${logDir}`);
                args.push(`--log-dir=${logDir}`);
            }

            log.info(`spawning ${kopiaPath} ${args.join(' ')}`);
            runningServerProcess = spawn(kopiaPath, args, {
            });
            this.raiseStatusUpdatedEvent();

            runningServerProcess.stdout.on('data', this.appendToLog.bind(this));
            runningServerProcess.stderr.on('data', this.detectServerParam.bind(this));

            const p = runningServerProcess;

            log.info('starting polling loop');

            const statusUpdated = this.raiseStatusUpdatedEvent.bind(this);

            function pollOnce() {
                if (!runningServerAddress || !runningServerCertificate || !runningServerPassword) {
                    return;
                }

                const req = https.request({
                    ca: [runningServerCertificate],
                    host: "localhost",
                    port: parseInt(new URL(runningServerAddress).port),
                    method: "GET",
                    path: "/api/v1/repo/status",
                    headers: {
                        'Authorization': 'Basic ' + Buffer.from("kopia:" + runningServerPassword).toString('base64')
                     }  
                }, (resp) => {
                    if (resp.statusCode === 200) {
                        resp.on('data', x => { 
                            try {
                                const newDetails = JSON.parse(x);
                                if (JSON.stringify(newDetails) !== JSON.stringify(runningServerStatusDetails)) {
                                    runningServerStatusDetails = newDetails;
                                    statusUpdated();
                                }
                            } catch (e) {
                                log.warn('unable to parse status JSON', e);
                            }
                        });
                    } else {
                        log.warn('error fetching status', resp.statusMessage);
                    }
                });
                req.on('error', (e)=>{
                    log.info('error fetching status', e);
                });
                req.end();
            }

            const statusPollInterval = setInterval(pollOnce, 3000);

            runningServerProcess.on('close', (code, signal) => {
                this.appendToLog(`child process exited with code ${code} and signal ${signal}`);
                if (runningServerProcess === p) {
                    clearInterval(statusPollInterval);

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

                    case "SERVER CERTIFICATE":
                        runningServerCertificate = Buffer.from(value, 'base64').toString('ascii');
                        this.raiseStatusUpdatedEvent();
                        break;

                    case "SERVER ADDRESS":
                        runningServerAddress = value;
                        this.raiseStatusUpdatedEvent();
                        break;

                    default:
                        log.error(`detectServerParam encountered an invalid key: ${key}`)
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
            runningServerCertificate = "";
            runningServerProcess = null;
            this.raiseStatusUpdatedEvent();
        },

        getServerAddress() {
            return runningServerAddress;
        },

        getServerCertSHA256() {
            return runningServerCertSHA256;
        },

        getServerPassword() {
            return runningServerPassword;
        },

        getServerStatusDetails() {
            return runningServerStatusDetails;
        },

        getServerStatus() {
            if (!runningServerProcess) {
                return "Stopped";
            }

            if (runningServerCertSHA256 && runningServerAddress && runningServerPassword) {
                return "Running";
            }

            return "Starting";
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
