const { ipcMain } = require('electron');

const maxLogLines = 100;
let serverLog = [];

function sendLogsUpdate(sender) {
    sender.send('logs-updated', serverLog.join(''));
}

ipcMain.on('subscribe-to-logs', (event, arg) => {
    sendLogsUpdate(event.sender);

    ipcMain.addListener('logs-updated-event', () => {
        sendLogsUpdate(event.sender);
    })
});

module.exports = {
    appendToLog(data) {
        const l = serverLog.push(data);
        if (l > maxLogLines) {
            serverLog.splice(0, 1);
        }

        ipcMain.emit('logs-updated-event');
        console.log(`${data}`);
    }
}