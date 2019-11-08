import React from 'react';
import logo from './kopia-flat.svg';
import './App.css';

import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-table/react-table.css'
 
import { SourcesTable } from "./SourcesTable";
import { PoliciesTable } from "./PoliciesTable";
import { SnapshotsTable } from "./SnapshotsTable";
import Navbar from 'react-bootstrap/Navbar';
import { NavLink } from 'react-router-dom';
import Nav from 'react-bootstrap/Nav';
import Container from 'react-bootstrap/Container';

import { withRouter } from "react-router";

import {
  BrowserRouter as Router,
  Switch,
  Route,
} from "react-router-dom";

const SnapshotsTableWithRouter = withRouter(SnapshotsTable);

function App() {
  return (
      <Router>
        <Navbar bg="light" expand="lg">
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

        <Container>
          <Switch>
            <Route path="/snapshots/single-source/">
              <SnapshotsTableWithRouter />
            </Route>
            <Route path="/snapshots/dir/">
              <p>not implemented: directory browser</p>
            </Route>
            <Route path="/snapshots/file/">
            <p>not implemented: file browser</p>
            </Route>
            <Route path="/snapshots">
              <SourcesTable />
            </Route>
            <Route path="/policies">
              <PoliciesTable />
            </Route>
            <Route exact path="/">
              <p>not implemented: Status</p>
            </Route>
          </Switch>
        </Container>        
      </Router>
  );
}

export default App;
