const { ipcMain } = require('electron');
const config = require('electron-json-config');

const { appendToLog } = require('./logging');
const { defaultServerBinary } = require('./utils');
const { spawn } = require('child_process');

let serverProcess = null;

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

    console.log(`spawning ${kopiaPath} ${args.join(' ')}`);
    serverProcess = spawn(kopiaPath, args);
    ipcMain.emit('server-status-updated');

    serverProcess.stdout.on('data', appendToLog);
    serverProcess.stderr.on('data', appendToLog);

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

    stopServer: stopServer,
}