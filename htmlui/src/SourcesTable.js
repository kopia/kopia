import axios from 'axios';
import React, { Component } from 'react';
import Dropdown from 'react-bootstrap/Dropdown';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import { Link } from 'react-router-dom';
import MyTable from './Table';
import { compare, ownerName, rfc3339TimestampForDisplay, sizeDisplayName } from './uiutil';
import Button from 'react-bootstrap/Button';

const allOwners = "(all)"

export class SourcesTable extends Component {
    constructor() {
        super();
        this.state = {
            sources: [],
            isLoading: false,
            error: null,

            selectedOwner: allOwners,
        };

        this.sync = this.sync.bind(this);
        this.fetchSourcesWithoutSpinner = this.fetchSourcesWithoutSpinner.bind(this);
    }

    componentDidMount() {
        this.setState({ isLoading: true });
        this.fetchSourcesWithoutSpinner();
        this.interval = window.setInterval(this.fetchSourcesWithoutSpinner, 5000);
    }

    componentWillUnmount() {
        window.clearInterval(this.interval);
    }

    fetchSourcesWithoutSpinner() {
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

    selectOwner(h) {
        this.setState({
            selectedOwner: h,
        });
    }

    sync() {
        this.setState({ isLoading: true });
        axios.post('/api/v1/repo/sync', {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            alert('failed');
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    render() {
        let { sources, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        let uniqueOwners = sources.reduce((a, d) => {
            const owner = ownerName(d.source);

            if (!a.includes(owner)) { a.push(owner); }
            return a;
        }, []);

        uniqueOwners.sort();

        if (this.state.selectedOwner !== allOwners) {
            sources = sources.filter(x => ownerName(x.source) === this.state.selectedOwner);
        };

        const columns = [{
            id: 'path',
            Header: 'Path',
            accessor: x => x.source,
            sortType: (a, b) => {
                const v = compare(a.original.source.path, b.original.source.path);
                if (v !== 0) {
                    return v;
                }

                return compare(ownerName(a.original.source), ownerName(b.original.source));
            },
            width: "",
            Cell: x => <Link to={'/snapshots/single-source?userName=' + x.cell.value.userName + '&host=' + x.cell.value.host + '&path=' + x.cell.value.path}>{x.cell.value.path}</Link>,
        }, {
            id: 'owner',
            Header: 'Owner',
            accessor: x => x.source.userName + '@' + x.source.host,
            width: 250,
        }, {
            id: 'lastSnapshotTime',
            Header: 'Last Snapshot',
            width: 250,
            accessor: x => x.lastSnapshotTime,
            Cell: x => rfc3339TimestampForDisplay(x.cell.value),
        }, {
            id: 'lastSnapshotSize',
            Header: 'Size',
            width: 300,
            accessor: x => x.lastSnapshotSize,
            Cell: x => sizeDisplayName(x.cell.value),
        }, {
            id: 'status',
            Header: 'Status',
            width: 100,
            accessor: x => x.status,
        }]

        return <div className="padded">
            <Row>
                <Dropdown>
                    <Dropdown.Toggle variant="success" id="dropdown-basic">
                        Owner: {this.state.selectedOwner}
                    </Dropdown.Toggle>

                    <Dropdown.Menu>
                        <Dropdown.Item onClick={() => this.selectOwner(allOwners)}>(all)</Dropdown.Item>
                        {uniqueOwners.map(v => <Dropdown.Item key={v} onClick={() => this.selectOwner(v)}>{v}</Dropdown.Item>)}
                    </Dropdown.Menu>
                </Dropdown>

                &nbsp;
                <Button variant="primary" onClick={this.sync}>Sync</Button>
            </Row>
            <hr />
            <Row>
                <MyTable data={sources} columns={columns} />
            </Row>
        </div>;
    }
}
