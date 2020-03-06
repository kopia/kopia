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

      "remoteServerAddress": "",
      "remoteServerPassword": "",
      "remoteServerCertificateSHA256": "",
    };
    this.updateState = this.updateState.bind(this);
    this.saveConfiguration = this.saveConfiguration.bind(this);

    this.validateRemoteServerAddress = this.validateRemoteServerAddress.bind(this);

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

  validateRemoteServerAddress() {
    return false;
  }

  isValidHttpsURL(v) {
    try {
      const url = new URL(v);
      return url.protocol === "https:";
    } catch (_) {
      return false;  
    }
  }

  isValidSHA256(v) {
    const re = /^[0-9A-Fa-f]{64}$/g;
    return re.test(v);
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
            </React.Fragment>
          </div>}
        <Form.Group controlId="connectToServer">
          <Form.Check type="radio" name="serverType" label="Connect To Remote Server" checked={this.state.remoteServer} onChange={() => this.setState({ 'remoteServer': true })} />
        </Form.Group>
        {this.state.remoteServer &&
          <div className="indented">
            <React.Fragment>
              <Form.Group controlId="remoteServerAddress">
                <Form.Label>Server Address</Form.Label>
                <Form.Control type="text" isInvalid={!this.isValidHttpsURL(this.state.remoteServerAddress)} placeholder="Enter address" value={this.state.remoteServerAddress} onChange={this.updateState} />
                <Form.Text className="text-muted">Enter server URL https://server:port</Form.Text>
                <Form.Control.Feedback type="invalid">Must be valid server url starting with https://</Form.Control.Feedback>
              </Form.Group>

              <Form.Group controlId="remoteServerPassword">
                <Form.Label>Server Password</Form.Label>
                <Form.Control type="password" isInvalid={!this.state.remoteServerPassword} placeholder="Password" value={this.state.remoteServerPassword} onChange={this.updateState} />
                <Form.Control.Feedback type="invalid">Password cannot be empty.</Form.Control.Feedback>
              </Form.Group>

              <Form.Group controlId="remoteServerCertificateSHA256">
                <Form.Label>Server Certificate Fingerprint</Form.Label>
                <Form.Control type="text" isInvalid={!this.isValidSHA256(this.state.remoteServerCertificateSHA256)} placeholder="server certificate fingerprint (SHA256)" value={this.state.remoteServerCertificateSHA256} onChange={this.updateState} />
                <Form.Control.Feedback type="invalid">Must be valid server fingerprint</Form.Control.Feedback>
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
