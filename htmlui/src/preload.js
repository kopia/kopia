const { dialog } = require("electron");

window.selectDirectory = function () {
    return dialog.showOpenDialogSync({
        properties: ['openDirectory']
    });
}
