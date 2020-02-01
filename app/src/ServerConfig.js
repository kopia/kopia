import React, { Component } from 'react';

import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';

export default class ServerConfig extends Component {
  constructor() {
    super();
    this.state = {
      "remoteServer": false,
      "kopiaPath": "",
      "configFile": "",
      "repositoryPassword": "",
      'serverUsername': 'kopia',
      "serverAddress": "https://localhost:51515",
      "serverPassword": "",
    };
    this.updateState = this.updateState.bind(this);
    this.saveConfiguration = this.saveConfiguration.bind(this);

    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.on('config-updated', (event, args) => {
        this.setState(args);
      })

      ipcRenderer.send('fetch-config');
    }
  }

  componentDidMount() {
  }

  onChangeServerType(e) {
  }

  updateState(e) {
    let d = {}
    d[e.target.id] = e.target.value;
    this.setState(d);
  }

  saveConfiguration() {
    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.sendSync('save-config', this.state);
    }
    window.close();
  }

  render() {
    return <>
      <Form>
        <Form.Group controlId="useBuiltInServer">
          <Form.Check type="radio" name="serverType" label="Internal Kopia Server" checked={!this.state.remoteServer} onChange={() => this.setState({ 'remoteServer': false })} />
        </Form.Group>
        {!this.state.remoteServer &&
          <div className="indented">
            <React.Fragment>
              <Form.Group controlId="kopiaPath">
                <Form.Label>Override Path To <code>kopia</code> executable</Form.Label>
                <Form.Control type="text" placeholder="Enter path" value={this.state.kopiaPath} onChange={this.updateState} />
                <Form.Text className="text-muted">Uses embedded executable if not set.</Form.Text>
              </Form.Group>

              <Form.Group controlId="configFile">
                <Form.Label>Override Configuration File</Form.Label>
                <Form.Control type="text" placeholder="Configuration file" value={this.state.configFile} onChange={this.updateState} />
                <Form.Text className="text-muted">Uses default configuration path, if not set.</Form.Text>
              </Form.Group>

              <Form.Group controlId="repositoryPassword">
                <Form.Label>Repository Password</Form.Label>
                <Form.Control type="password" placeholder="Password" value={this.state.repositoryPassword} onChange={this.updateState} />
                <Form.Text className="text-muted">Uses saved password if not set.</Form.Text>
              </Form.Group>
            </React.Fragment>
          </div>}
        <Form.Group controlId="connectToServer">
          <Form.Check type="radio" name="serverType" label="Connect To Server" checked={this.state.remoteServer} onChange={() => this.setState({ 'remoteServer': true })} />
        </Form.Group>
        {this.state.remoteServer &&
          <div className="indented">
            <React.Fragment>
              <Form.Group controlId="serverAddress">
                <Form.Label>Server Address</Form.Label>
                <Form.Control type="text" placeholder="Enter address" value={this.state.serverAddress} onChange={this.updateState} />
                <Form.Text className="text-muted">To connect to local server, use localhost:51515</Form.Text>
              </Form.Group>

              <Form.Group controlId="serverUsername">
                <Form.Label>Server Username</Form.Label>
                <Form.Control type="text" placeholder="Enter username" value={this.state.serverUsername} onChange={this.updateState} />
              </Form.Group>

              <Form.Group controlId="serverPassword">
                <Form.Label>Server Password</Form.Label>
                <Form.Control type="password" placeholder="Password" value={this.state.serverPassword} onChange={this.updateState} />
              </Form.Group>
            </React.Fragment>
          </div>
        }
      </Form>
      <hr />
      <Button variant="success" type="submit" onClick={this.saveConfiguration}>Save Configuration</Button>
      &nbsp;
      <Button variant="secondary" type="submit" onClick={window.close}>Cancel</Button>
    </>;
  }
}
