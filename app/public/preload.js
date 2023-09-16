const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("kopiaUI", {
    "selectDirectory": function (onSelected) {
        ipcRenderer.invoke('select-dir').then(v => {
            onSelected(v);
        });
    },
    "browseDirectory": function(path) {
        ipcRenderer.invoke('browse-dir', path);
    },
})

