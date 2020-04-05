import React, { Component } from 'react';

import Form from 'react-bootstrap/Form';

export default class ServerLogs extends Component {
    constructor(props) {
        super();
        this.state = {
            logs: "",
        };

        if (window.require) {
            const { ipcRenderer } = window.require('electron');

            const repoID = props.repoID;

            ipcRenderer.on('logs-updated-event', (event, args) => {
                if (args.repoID === repoID) {
                    this.setState({logs:args.logs});
                }
            })
        }
    }

    render() {
        return <Form.Group controlId="logs">
            <Form.Control as="textarea" rows="30" value={this.state.logs} />
        </Form.Group>;
    }
}
