import 'bootstrap/dist/css/bootstrap.min.css';
import React, { Component } from 'react';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';
import Nav from 'react-bootstrap/Nav';
import Row from 'react-bootstrap/Row';
import Tab from 'react-bootstrap/Tab';

import { v4 } from 'uuid';

import './App.css';
import Repo from './Repo';

export default class App extends Component {
  constructor() {
    super();
    this.state = {
      sortedConfigs: [],
      activeTabKey: "",
    };

    this.addNewServer = this.addNewServer.bind(this);

    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.on('config-list-updated-event', (event, arg) => this.configListUpdated(arg));
      ipcRenderer.send('config-list-fetch')
    }
  }

  configListUpdated(sortedConfigs) {
    let ak = this.state.activeTabKey;

    if (!sortedConfigs.find(e => e.repoID === ak)) {
      ak = "";
    }

    if (!ak && sortedConfigs.length > 0) {
      ak = sortedConfigs[0].repoID;
    }

    this.setState({
      sortedConfigs: sortedConfigs,
      activeTabKey: ak,
    });
  }

  addNewServer() {
    let newRepoID = v4();

    if (!this.state.sortedConfigs) {
      newRepoID = "default";
    }
    
    const newConfigs = this.state.sortedConfigs.concat([{
      repoID: newRepoID,
      desc: "<New Repository>",
    }])

    this.setState({
      sortedConfigs: newConfigs,
      activeTabKey: newRepoID,
    })

    if (window.require) {
      const { ipcRenderer } = window.require('electron');

      ipcRenderer.sendSync('config-add', {
        repoID: newRepoID,
        data: {
          configFile: newRepoID + ".config",
        },
      });
    }
  }

  render() {
    return (
      <Container fluid>
        <hr />
        <Tab.Container id="left-tabs-example" activeKey={this.state.activeTabKey} onSelect={(k) => this.setState({ activeTabKey: k })}>
          <Row>
            <Col sm={2}>
              <Nav variant="pills" className="flex-column">
                {this.state.sortedConfigs.map(v => <Nav.Item key={v.repoID} >
                  <Nav.Link eventKey={v.repoID}>{v.desc || v.repoID}</Nav.Link>
                </Nav.Item>)}
                <Nav.Item>
                  <Nav.Link onClick={this.addNewServer}>+ New</Nav.Link>
                </Nav.Item>
              </Nav>
            </Col>
            <Col sm={10}>
              <Tab.Content>
                {this.state.sortedConfigs.map(v => <Tab.Pane key={v.repoID} eventKey={v.repoID}>
                  <Repo repoID={v.repoID} />
                </Tab.Pane>)}
              </Tab.Content>
            </Col>
          </Row>
        </Tab.Container>

        {/* {JSON.stringify(this.state)} */}

      </Container>
    );
  }
};
