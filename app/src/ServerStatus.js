import React, { Component } from 'react';

export default class ServerStatus extends Component {
    constructor() {
        super();
        this.state = {
            running: false,
        };

        if (window.require) {
            const { ipcRenderer } = window.require('electron');

            ipcRenderer.on('status-updated', (event, args) => {
                this.setState(args);
            })

            ipcRenderer.send('subscribe-to-status');
        }
    }

    render() {
        return <div>Server: <code>{this.state.status}</code><br/>URL: <code>{this.state.serverAddress}</code><br/>TLS Certificate Fingerprint: <code>{this.state.serverCertSHA256}</code></div>;
    }
}
