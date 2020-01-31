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
        return <div>Server: <b>{this.state.status}</b> on <code>{this.state.serverAddress}</code></div>;
    }
}
