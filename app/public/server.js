import { ipcMain } from 'electron';
const path = await import('path');
const https = await import('https');

import { defaultServerBinary } from './utils.js';
import { spawn } from 'child_process';
import log from "electron-log";
import { configDir, isPortableConfig } from './config.js';

let servers = {};

function newServerForRepo(repoID) {
    let runningServerProcess = null;
    let runningServerCertSHA256 = "";
    let runningServerPassword = "";
    let runningServerControlPassword = "";
    let runningServerAddress = "";
    let runningServerCertificate = "";
    let runningServerStatusDetails = {
        startingUp: true,
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

            args.push('server', 'start', '--ui',
                '--tls-print-server-cert',
                '--tls-generate-cert-name=127.0.0.1',
                '--random-password',
                '--random-server-control-password',
                '--tls-generate-cert',
                '--async-repo-connect',
                '--shutdown-on-stdin', // shutdown the server when parent dies
                '--address=127.0.0.1:0');
    

            args.push("--config-file", path.resolve(configDir(), repoID + ".config"));
            if (isPortableConfig()) {
                const cacheDir = path.resolve(configDir(), "cache", repoID);
                const logsDir = path.resolve(configDir(), "logs", repoID);
                args.push("--cache-directory", cacheDir);
                args.push("--log-dir", logsDir);
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

	    const pollInterval = 3000; 

            function pollOnce() {
                if (!runningServerAddress || !runningServerCertificate || !runningServerPassword || !runningServerControlPassword) {
                    return;
                }

                const req = https.request({
                    ca: [runningServerCertificate],
                    host: "127.0.0.1",
                    port: parseInt(new URL(runningServerAddress).port),
                    method: "GET",
                    path: "/api/v1/control/status",
		    timeout: pollInterval,
                    headers: {
                        'Authorization': 'Basic ' + Buffer.from("server-control" + ':' + runningServerControlPassword).toString('base64')
                     }  
                }, (resp) => {
                    if (resp.statusCode === 200) {
                        resp.on('data', x => { 
                            try {
                                const newDetails = JSON.parse(x);
                                if (JSON.stringify(newDetails) != JSON.stringify(runningServerStatusDetails)) {
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

            const statusPollInterval = setInterval(pollOnce, pollInterval);

            runningServerProcess.on('close', (code, signal) => {
                this.appendToLog(`child process exited with code ${code} and signal ${signal}`);
                if (runningServerProcess === p) {
                    clearInterval(statusPollInterval);

                    runningServerAddress = "";
                    runningServerPassword = "";
                    runningServerControlPassword = "";
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

                    case "SERVER CONTROL PASSWORD":
                        runningServerControlPassword = value;
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

export function serverForRepo(repoID) {
        let s = servers[repoID];
        if (s) {
            return s;
        }

        s = newServerForRepo(repoID);
        servers[repoID] = s;
        return s;
    }

