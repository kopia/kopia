const { ipcMain } = require('electron');
const config = require('electron-json-config');

const { appendToLog } = require('./logging');
const { defaultServerBinary } = require('./utils');
const { spawn } = require('child_process');

let serverProcess = null;
let serverCertSHA256 = "";
let serverPassword = "";
let serverAddress = "";

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
                serverPassword = value;
                break;

            case "SERVER CERT SHA256":
                serverCertSHA256 = value;
                break;

            case "SERVER ADDRESS":
                serverAddress = value;
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
    // args.push('--auto-shutdown=600s')

    console.log(`spawning ${kopiaPath} ${args.join(' ')}`);
    serverProcess = spawn(kopiaPath, args);
    ipcMain.emit('server-status-updated');

    serverProcess.stdout.on('data', appendToLog);
    serverProcess.stderr.on('data', detectServerParam);

    serverProcess.on('close', (code, signal) => {
        appendToLog(`child process exited with code ${code} and signal ${signal}`);
        serverProcess = null;
        ipcMain.emit('server-status-updated');
    });
}

function stopServer() {
    if (!serverProcess) {
        console.log('stopServer: server not started');
        return;
    }

    serverProcess.kill();
    serverProcess = null;
}

ipcMain.on('subscribe-to-status', (event, arg) => {
    sendStatusUpdate(event.sender);

    ipcMain.addListener('status-updated-event', () => {
        sendStatusUpdate(event.sender);
    })
});

function getServerStatus() {
    if (!serverProcess) {
        return "Stopped";
    }

    return "Running";
};

function getServerAddress() {
    return "localhost:51515";
};

function sendStatusUpdate(sender) {
    sender.send('status-updated', {
        status: getServerStatus(),
        serverAddress: getServerAddress(),
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
        return serverAddress;
    },

    getServerCertSHA256() {
        return serverCertSHA256;
    },

    getServerPassword() {
        return serverPassword;
    },

    stopServer: stopServer,
}