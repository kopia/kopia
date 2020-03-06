import React from 'react';
import './App.css';
import Container from 'react-bootstrap/Container';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import logo from './kopia-flat.svg';

import ServerConfig from './ServerConfig.js';
import ServerLogs from './ServerLogs';
import ServerStatus from './ServerStatus';

import 'bootstrap/dist/css/bootstrap.min.css';

function App() {
  return (
    <Container fluid>
      <header>
        <img src={logo} className="App-logo" alt="logo" />
        <span className="title">Kopia</span>
      </header>
      <hr/>
      <Tabs defaultActiveKey="config" transition={false}>
        <Tab eventKey="config" title="Server Configuration">
          <div className="tab-body">
            <ServerConfig />
          </div>
        </Tab>
        <Tab eventKey="logs" title="Logs">
          <div className="tab-body">
            <ServerLogs />
          </div>
        </Tab>
      </Tabs>
      <hr/>
      <ServerStatus />
    </Container>
  );
}

export default App;
