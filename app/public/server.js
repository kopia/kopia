const { ipcMain } = require('electron');
const config = require('electron-json-config');

const { appendToLog } = require('./logging');
const { defaultServerBinary } = require('./utils');
const { spawn } = require('child_process');

let runningServerProcess = null;
let runningServerCertSHA256 = "";
let runningServerPassword = "";
let runningServerAddress = "";

function detectServerParam(data) {
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
                break;

            case "SERVER CERT SHA256":
                runningServerCertSHA256 = value;
                break;

            case "SERVER ADDRESS":
                runningServerAddress = value;
                break;
        }
    }

    appendToLog(data);
}

function startServer() {
    let kopiaPath = config.get('kopiaPath');
    if (!kopiaPath) {
        kopiaPath = defaultServerBinary();
    }

    let args = [];

    let configFile = config.get('configFile');
    if (configFile) {
        args.push("--config", configFile);
    }

    args.push('server', '--ui');

    args.push('--random-password')
    args.push('--tls-generate-cert')
    args.push('--address=localhost:0')
    // args.push('--auto-shutdown=600s')

    console.log(`spawning ${kopiaPath} ${args.join(' ')}`);
    runningServerProcess = spawn(kopiaPath, args);
    ipcMain.emit('server-status-updated');

    runningServerProcess.stdout.on('data', appendToLog);
    runningServerProcess.stderr.on('data', detectServerParam);

    runningServerProcess.on('close', (code, signal) => {
        appendToLog(`child process exited with code ${code} and signal ${signal}`);
        runningServerProcess = null;
        ipcMain.emit('server-status-updated');
    });
}

function stopServer() {
    if (!runningServerProcess) {
        console.log('stopServer: server not started');
        return;
    }

    runningServerProcess.kill();
    runningServerProcess = null;
}

ipcMain.on('subscribe-to-status', (event, arg) => {
    sendStatusUpdate(event.sender);

    ipcMain.addListener('status-updated-event', () => {
        sendStatusUpdate(event.sender);
    })
});

function getServerStatus() {
    if (config.get('remoteServer')) {
        return "Remote";
    } else {
        if (!runningServerProcess) {
            return "Stopped";
        }

        return "Running";
    }
};

function getActiveServerAddress() {
    if (config.get('remoteServer')) {
        return config.get('remoteServerAddress');
    } else {
        return runningServerAddress;
    }
};


function getActiveServerCertSHA256() {
    if (config.get('remoteServer')) {
        return config.get('remoteServerCertificateSHA256');
    } else {
        return runningServerCertSHA256;
    }
};

function getActiveServerPassword() {
    if (config.get('remoteServer')) {
        return config.get('remoteServerPassword');
    } else {
        return runningServerPassword;
    }
};

function sendStatusUpdate(sender) {
    sender.send('status-updated', {
        status: getServerStatus(),
        serverAddress: getActiveServerAddress() || "<pending>",
        serverCertSHA256: getActiveServerCertSHA256() || "<pending>",
    });
}

module.exports = {
    actuateServer() {
        stopServer();
        if (!config.get('remoteServer')) {
            startServer();
        }
    },

    getServerAddress() {
        return getActiveServerAddress();
    },

    getServerCertSHA256() {
        return getActiveServerCertSHA256();
    },

    getServerPassword() {
        return getActiveServerPassword();
    },

    stopServer: stopServer,
}