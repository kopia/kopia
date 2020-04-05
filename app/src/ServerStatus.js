import React, { Component } from 'react';

export default class ServerStatus extends Component {
    constructor(props) {
        super();
        this.state = {
            running: false,
        };

        if (window.require) {
            const { ipcRenderer } = window.require('electron');

            const repoID = props.repoID;

            ipcRenderer.on('status-updated-event', (event, args) => {
                if (args.repoID === repoID) {
                    this.setState(args);
                }
            })

            ipcRenderer.send('status-fetch', { repoID: repoID })
        }
    }

    render() {
        return <div>Server ({this.props.repoID}): <code>{this.state.status}</code><br/>URL: <code>{this.state.serverAddress}</code><br/>TLS Certificate Fingerprint: <code>{this.state.serverCertSHA256}</code></div>;
    }
}
