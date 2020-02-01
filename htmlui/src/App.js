import React from 'react';
import logo from './kopia-flat.svg';
import './App.css';

import 'bootstrap/dist/css/bootstrap.min.css';
 
import { SourcesTable } from "./SourcesTable";
import { PoliciesTable } from "./PoliciesTable";
import { SnapshotsTable } from "./SnapshotsTable";
import { DirectoryObject } from "./DirectoryObject";
import Navbar from 'react-bootstrap/Navbar';
import { NavLink } from 'react-router-dom';
import Nav from 'react-bootstrap/Nav';
import Container from 'react-bootstrap/Container';

import {
  BrowserRouter as Router,
  Switch,
  Route,
} from "react-router-dom";

function App() {
  return (
      <Router>
        <Navbar bg="light" expand="sm">
          <Navbar.Brand href="/"><img src={logo} className="App-logo" alt="logo" /></Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="mr-auto">
              <NavLink className="nav-link" activeClassName="active" exact to="/">Status</NavLink>
              <NavLink className="nav-link" activeClassName="active" to="/snapshots">Snapshots</NavLink>
              <NavLink className="nav-link" activeClassName="active" to="/policies">Policies</NavLink>
            </Nav>
          </Navbar.Collapse>
        </Navbar>

        <Container fluid>
          <Switch>
            <Route path="/snapshots/single-source/" component={SnapshotsTable} />
            <Route path="/snapshots/dir/:oid" component={DirectoryObject} />
            <Route path="/snapshots" component={SourcesTable} />
            <Route path="/policies" component={PoliciesTable} />
            <Route exact path="/">
              <p>not implemented: Status</p>
            </Route>
          </Switch>
        </Container>        
      </Router>
  );
}

export default App;
