import React, { Component } from 'react';
import ReactTable from 'react-table';
import axios from 'axios';

import {
    Link
} from 'react-router-dom';

import Dropdown from 'react-bootstrap/Dropdown';
import Spinner from 'react-bootstrap/Spinner';
import Row from 'react-bootstrap/Row';

const allHosts = "(all)"
const allUsers = "(all)"

export class SourcesTable extends Component {
    constructor() {
        super();
        this.state = {
            sources: [],
            isLoading: false,
            error: null,

            selectedHost: allHosts,
            selectedUser: allUsers,
        };
    }

    componentDidMount() {
        this.setState({ isLoading: true });
        axios.get('/api/v1/sources').then(result => {
            this.setState({
                sources: result.data.sources,
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    selectHost(h) {
        this.setState({
            selectedHost: h,
        });
    }

    selectUser(u) {
        this.setState({
            selectedUser: u,
        });
    }

    hostClicked(h) {
        alert('host clicked ' + h);
    }

    render() {
        let { sources, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        let uniqueHosts = sources.reduce((a, d) => {
            if (!a.includes(d.source.host)) { a.push(d.source.host); }
            return a;
          }, []);

        uniqueHosts.sort();

        let uniqueUsers = sources.reduce((a, d) => {
            if (!a.includes(d.source.userName)) { a.push(d.source.userName); }
            return a;
          }, []);

        uniqueUsers.sort();

        if (this.state.selectedHost !== allHosts) {
            sources = sources.filter(x => x.source.host === this.state.selectedHost);
        };

        if (this.state.selectedUser !== allUsers) {
            sources = sources.filter(x => x.source.userName === this.state.selectedUser);
        };

        const columns = [{
            id: 'host',
            Header: 'Host',
            accessor: 'source.host',
            width: 150,
        }, {
            id: 'user',
            Header: 'User',
            accessor: 'source.userName',
            width: 150,
        }, {
            id: 'path',
            Header: 'Path',
            accessor: x => x.source,
            Cell: x => <Link to={'/snapshots/single-source?userName=' + x.value.userName + '&host=' + x.value.host + '&path=' + x.value.path}>{x.value.path}</Link>,
            width: 600,
        }, {
            id: 'lastSnapshotTime',
            Header: 'Last Snapshot',
            width: 200,
            accessor: x => x.lastSnapshotTime,
        }, {
            id: 'lastSnapshotSize',
            Header: 'Size',
            width: 100,
            accessor: x => x.lastSnapshotSize,
        }]

        return <div>
            <Row>
<Dropdown>
  <Dropdown.Toggle variant="primary" id="dropdown-basic">
    Host: {this.state.selectedHost}
  </Dropdown.Toggle>

  <Dropdown.Menu>
      <Dropdown.Item onClick={() => this.selectHost(allHosts)}>(all)</Dropdown.Item>
      {uniqueHosts.map(v => <Dropdown.Item onClick={() => this.selectHost(v)}>{v}</Dropdown.Item>)}
  </Dropdown.Menu>
</Dropdown>

&nbsp;
<Dropdown>
  <Dropdown.Toggle variant="primary" id="dropdown-basic">
    User: {this.state.selectedUser}
  </Dropdown.Toggle>

  <Dropdown.Menu>
      <Dropdown.Item onClick={() => this.selectUser(allUsers)}>(all)</Dropdown.Item>
      {uniqueUsers.map(v => <Dropdown.Item onClick={() => this.selectUser(v)}>{v}</Dropdown.Item>)}
  </Dropdown.Menu>
</Dropdown>
</Row>

<p></p>
            <Row><ReactTable data={sources} columns={columns} /></Row>
        </div>;

    }
}
