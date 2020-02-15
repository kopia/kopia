import 'bootstrap/dist/css/bootstrap.min.css';
import React from 'react';
import Container from 'react-bootstrap/Container';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import { BrowserRouter as Router, NavLink, Route, Switch } from 'react-router-dom';
import './App.css';
import { DirectoryObject } from "./DirectoryObject";
import logo from './kopia-flat.svg';
import { PoliciesTable } from "./PoliciesTable";
import { RepoStatus } from "./RepoStatus";
import { SnapshotsTable } from "./SnapshotsTable";
import { SourcesTable } from "./SourcesTable";

function App() {
  return (
      <Router>
        <Navbar bg="light" expand="sm">
          <Navbar.Brand href="/"><img src={logo} className="App-logo" alt="logo" /></Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="mr-auto">
              <NavLink className="nav-link" activeClassName="active" to="/snapshots">Snapshots</NavLink>
              <NavLink className="nav-link" activeClassName="active" to="/policies">Policies</NavLink>
              <NavLink className="nav-link" activeClassName="active" exact to="/">Repository</NavLink>
            </Nav>
          </Navbar.Collapse>
        </Navbar>

        <Container fluid>
          <Switch>
            <Route path="/snapshots/single-source/" component={SnapshotsTable} />
            <Route path="/snapshots/dir/:oid" component={DirectoryObject} />
            <Route path="/snapshots" component={SourcesTable} />
            <Route path="/policies" component={PoliciesTable} />
            <Route exact path="/" component={RepoStatus} />
          </Switch>
        </Container>        
      </Router>
  );
}

export default App;
