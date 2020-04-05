import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Accordion from 'react-bootstrap/Accordion';
import Card from 'react-bootstrap/Card';

export default class ServerConfig extends Component {
  constructor(props) {
    super(props);
    this.state = {
      validated: false,
      config: {},
    };

    this.updateState = this.updateState.bind(this);
    this.applyConfiguration = this.applyConfiguration.bind(this);
    this.deleteConfiguration = this.deleteConfiguration.bind(this);
    this.fetchConfig = this.fetchConfig.bind(this);
    this.setConfigState = this.setConfigState.bind(this);

    this.validate = this.validate.bind(this);
  }

  componentDidMount() {
    this.fetchConfig();
  }

  fetchConfig() {
    if (!window.require) {
      return;
    }

    const { ipcRenderer } = window.require('electron');
    const cfg = ipcRenderer.sendSync('config-get', { repoID: this.props.repoID });
    console.log('got config', cfg);
    this.setState({
      config: cfg,
      validated: false,
    });
  }

  onChangeServerType(e) {
  }

  setConfigState(cfg) {
    let d = { ...this.state.config };
    for (let k in cfg) {
      d[k] = cfg[k];
    }
    this.setState({
      config: d,
    });
  }

  updateState(e) {
    let d = { ...this.state.config };
    d[e.target.id] = e.target.value;
    this.setState({
      config: d,
    });
  }

  isDescriptionValid() {
    return this.state.config.description !== "";
  }

  isRemoteServerAddressValid() {
    if (!this.state.config.remoteServer) {
      return true;
    }

    return this.isValidHttpsURL(this.state.config.remoteServerAddress);
  }

  isRemoteServerPasswordValid() {
    if (!this.state.config.remoteServer) {
      return true;
    }

    return !!this.state.config.remoteServerPassword;
  }

  isRemoteServerCertValid() {
    if (!this.state.config.remoteServer) {
      return true;
    }

    return this.isValidSHA256(this.state.config.remoteServerCertificateSHA256);
  }

  validate() {
    if (!this.isDescriptionValid()) {
      return false;
    }

    if (!this.isRemoteServerAddressValid()) {
      return false;
    }

    if (!this.isRemoteServerPasswordValid()) {
      return false;
    }

    if (!this.isRemoteServerCertValid()) {
      return false;
    }

    return true;
  }

  applyConfiguration() {
    this.setState({
      validated: true,
    });

    if (!this.validate()) {
      return;
    }

    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.sendSync('config-save', {
        repoID: this.props.repoID,
        config: this.state.config
      });
    }
  }

  deleteConfiguration() {
    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.sendSync('config-delete', {
        repoID: this.props.repoID,
      });
    }
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
        <Form.Group controlId="description">
          <Form.Label>Description</Form.Label>
          <Form.Control type="text" isInvalid={this.state.validated && !this.isDescriptionValid()} placeholder="Enter repository description" value={this.state.config.description} onChange={this.updateState} />
          <Form.Control.Feedback type="invalid">Description must be provided</Form.Control.Feedback>
        </Form.Group>
        <Form.Group controlId="useBuiltInServer">
          <Form.Check type="radio" name="serverType" label="Start Local Server" checked={!this.state.config.remoteServer} onChange={() => this.setConfigState({ remoteServer: false })} />
        </Form.Group>
        {!this.state.config.remoteServer &&
          <div className="indented">
            <Accordion>
              <Card>
                <Card.Header>
                  <Accordion.Toggle as={Button} variant="link" eventKey="showAdvanced">Show Advanced Options</Accordion.Toggle>
                </Card.Header>
                <Accordion.Collapse eventKey="showAdvanced">
                  <Card.Body>
                    <React.Fragment>
                      <Form.Group controlId="kopiaPath">
                        <Form.Label>Override Path To <code>kopia</code> executable</Form.Label>
                        <Form.Control type="text" placeholder="Enter path" value={this.state.config.kopiaPath} onChange={this.updateState} />
                        <Form.Text className="text-muted">Uses embedded executable if not set.</Form.Text>
                      </Form.Group>

                      <Form.Group controlId="configFile">
                        <Form.Label>Override Configuration File</Form.Label>
                        <Form.Control type="text" placeholder="Configuration file" value={this.state.config.configFile} onChange={this.updateState} />
                        <Form.Text className="text-muted">Uses default configuration path, if not set.</Form.Text>
                      </Form.Group>
                    </React.Fragment></Card.Body>
                </Accordion.Collapse>
              </Card>
            </Accordion>
          </div>}
        <Form.Group controlId="connectToServer">
          <Form.Check type="radio" name="serverType" label="Connect To Remote Server" checked={this.state.config.remoteServer} onChange={() => this.setConfigState({ remoteServer: true })} />
        </Form.Group>
        {this.state.config.remoteServer &&
          <div className="indented">
            <React.Fragment>
              <Form.Group controlId="remoteServerAddress">
                <Form.Label>Server Address</Form.Label>
                <Form.Control type="text" isInvalid={this.state.validated && !this.isRemoteServerAddressValid()} placeholder="Enter address" value={this.state.config.remoteServerAddress} onChange={this.updateState} />
                <Form.Text className="text-muted">Enter server URL https://server:port</Form.Text>
                <Form.Control.Feedback type="invalid">Must be valid server url starting with https://</Form.Control.Feedback>
              </Form.Group>

              <Form.Group controlId="remoteServerPassword">
                <Form.Label>Server Password</Form.Label>
                <Form.Control type="password" isInvalid={this.state.validated && !this.isRemoteServerPasswordValid()} placeholder="Password" value={this.state.config.remoteServerPassword} onChange={this.updateState} />
                <Form.Control.Feedback type="invalid">Password cannot be empty.</Form.Control.Feedback>
              </Form.Group>

              <Form.Group controlId="remoteServerCertificateSHA256">
                <Form.Label>Server Certificate Fingerprint</Form.Label>
                <Form.Control type="text" isInvalid={this.state.validated && !this.isRemoteServerCertValid()} placeholder="server certificate fingerprint (SHA256)" value={this.state.config.remoteServerCertificateSHA256} onChange={this.updateState} />
                <Form.Control.Feedback type="invalid">Must be valid server fingerprint</Form.Control.Feedback>
              </Form.Group>
            </React.Fragment>
          </div>
        }
      </Form>
      <hr />
      <Button variant="success" type="submit" onClick={this.applyConfiguration}>Apply and Restart</Button>
      &nbsp;
      <Button variant="warning" onClick={this.fetchConfig}>Reset</Button>
      &nbsp;
      <Button variant="danger" onClick={this.deleteConfiguration}>Delete</Button>

      {/* {JSON.stringify(this.state)} */}
    </>;
  }
}
