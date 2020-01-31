import React, { Component } from 'react';

import Form from 'react-bootstrap/Form';

export default class ServerLogs extends Component {
    constructor() {
        super();
        this.state = {
            logs: "",
        };

        if (window.require) {
            const { ipcRenderer } = window.require('electron');

            ipcRenderer.on('logs-updated', (event, args) => {
                this.setState({logs:args});
            })

            ipcRenderer.send('subscribe-to-logs');
        }
    }

    render() {
        return <Form.Group controlId="logs">
            <Form.Control as="textarea" rows="30" value={this.state.logs} />
        </Form.Group>;
    }
}
