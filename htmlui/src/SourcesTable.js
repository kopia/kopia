import { faSync, faUserFriends } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import moment from 'moment';
import React, { Component } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Dropdown from 'react-bootstrap/Dropdown';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import { Link } from 'react-router-dom';
import { handleChange } from './forms';
import MyTable from './Table';
import { compare, errorAlert, ownerName, redirectIfNotConnected, sizeDisplayName, sizeWithFailures, sourceQueryStringParams } from './uiutil';

const localSnapshots = "Local Snapshots"
const allSnapshots = "All Snapshots"

export class SourcesTable extends Component {
    constructor() {
        super();
        this.state = {
            sources: [],
            isLoading: false,
            error: null,

            localSourceName: "",
            multiUser: false,
            selectedOwner: localSnapshots,
            selectedDirectory: "",
        };

        this.sync = this.sync.bind(this);
        this.fetchSourcesWithoutSpinner = this.fetchSourcesWithoutSpinner.bind(this);
        this.handleChange = handleChange.bind(this);

        this.cancelSnapshot = this.cancelSnapshot.bind(this);
        this.startSnapshot = this.startSnapshot.bind(this);
    }

    componentDidMount() {
        this.setState({ isLoading: true });
        this.fetchSourcesWithoutSpinner();
        this.interval = window.setInterval(this.fetchSourcesWithoutSpinner, 3000);
    }

    componentWillUnmount() {
        window.clearInterval(this.interval);
    }

    fetchSourcesWithoutSpinner() {
        axios.get('/api/v1/sources').then(result => {
            this.setState({
                localSourceName: result.data.localUsername + "@" + result.data.localHost,
                multiUser: result.data.multiUser,
                sources: result.data.sources,
                isLoading: false,
            });
        }).catch(error => {
            redirectIfNotConnected(error);
            this.setState({
                error,
                isLoading: false
            });
        });
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
            errorAlert(error);
            this.setState({
                error,
                isLoading: false
            });
        });
    }


    statusCell(x, parent) {
        switch (x.cell.value) {
            case "IDLE":
                return <>
                    <Button variant="primary" size="sm" onClick={() => {
                        parent.startSnapshot(x.row.original.source);
                    }}>Snapshot now</Button>
                </>;

            case "PENDING":
                return <>
                    <Spinner animation="border" variant="secondary" size="sm" title="Snapshot will after the previous snapshot completes" />
                    &nbsp;Pending
                </>;

            case "UPLOADING":
                let u = x.row.original.upload;
                let title = "";
                let totals = "";
                if (u) {
                    title = " hashed " + u.hashedFiles + " files (" + sizeDisplayName(u.hashedBytes) + ")\n" +
                        " cached " + u.cachedFiles + " files (" + sizeDisplayName(u.cachedBytes) + ")\n" +
                        " dir " + u.directory;

                    const totalBytes = u.hashedBytes + u.cachedBytes;

                    totals = sizeDisplayName(totalBytes);
                    if (u.estimatedBytes) {
                        totals += "/" + sizeDisplayName(u.estimatedBytes);

                        const percent = Math.round(totalBytes * 1000.0 / u.estimatedBytes) / 10.0;
                        if (percent <= 100) {
                            totals += " " + percent + "%";
                        }
                    }
                }

                return <>
                    <Spinner animation="border" variant="primary" size="sm" title={title} />&nbsp;{totals}
                    &nbsp;
                    {x.row.original.currentTask && <Link to={"/tasks/" + x.row.original.currentTask}>Details</Link>}
                </>;

            default:
                return "";
        }
    }

    cancelSnapshot(source) {
        axios.post('/api/v1/sources/cancel?' + sourceQueryStringParams(source), {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            errorAlert(error);
        });
    }

    startSnapshot(source) {
        axios.post('/api/v1/sources/upload?' + sourceQueryStringParams(source), {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            errorAlert(error);
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

        switch (this.state.selectedOwner) {
            case allSnapshots:
                // do nothing;
                break;

            case localSnapshots:
                sources = sources.filter(x => ownerName(x.source) === this.state.localSourceName);
                break;

            default:
                sources = sources.filter(x => ownerName(x.source) === this.state.selectedOwner);
                break;
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
            Cell: x => <Link to={'/snapshots/single-source?' + sourceQueryStringParams(x.cell.value)}>{x.cell.value.path}</Link>,
        }, {
            id: 'owner',
            Header: 'Owner',
            accessor: x => x.source.userName + '@' + x.source.host,
            width: 250,
        }, {
            id: 'lastSnapshotSize',
            Header: 'Size',
            width: 120,
            accessor: x => x.lastSnapshot ? x.lastSnapshot.stats.totalSize : 0,
            Cell: x => sizeWithFailures(
                x.cell.value,
                x.row.original.lastSnapshot && x.row.original.lastSnapshot.rootEntry ? x.row.original.lastSnapshot.rootEntry.summ : null),
        }, {
            id: 'lastSnapshotTime',
            Header: 'Last Snapshot',
            width: 160,
            accessor: x => x.lastSnapshot ? x.lastSnapshot.startTime : null,
            Cell: x => x.cell.value ? <p title={moment(x.cell.value).toLocaleString()}>{moment(x.cell.value).fromNow()}</p> : '',
        }, {
            id: 'nextSnapshotTime',
            Header: 'Next Snapshot',
            width: 160,
            accessor: x => x.nextSnapshotTime,
            Cell: x => (x.cell.value && x.row.original.status !== "UPLOADING") ? <>
                <p title={moment(x.cell.value).toLocaleString()}>{moment(x.cell.value).fromNow()}
                    {moment(x.cell.value).isBefore(moment()) && <>
                        &nbsp;
                    <Badge variant="secondary">overdue</Badge>
                    </>}
                </p>
            </> : '',
        }, {
            id: 'status',
            Header: 'Status',
            width: 300,
            accessor: x => x.status,
            Cell: x => this.statusCell(x, this),
        }]

        return <div className="padded">
            <div className="list-actions">
                <Row>
                    {this.state.multiUser && <><Col xs="auto">
                        <Dropdown>
                            <Dropdown.Toggle size="sm" variant="outline-primary" id="dropdown-basic">
                                <FontAwesomeIcon icon={faUserFriends} />&nbsp;{this.state.selectedOwner}
                            </Dropdown.Toggle>

                            <Dropdown.Menu>
                                <Dropdown.Item onClick={() => this.selectOwner(localSnapshots)}>{localSnapshots}</Dropdown.Item>
                                <Dropdown.Item onClick={() => this.selectOwner(allSnapshots)}>{allSnapshots}</Dropdown.Item>
                                <Dropdown.Divider />
                                {uniqueOwners.map(v => <Dropdown.Item key={v} onClick={() => this.selectOwner(v)}>{v}</Dropdown.Item>)}
                            </Dropdown.Menu>
                        </Dropdown>
                    </Col></>}
                    <Col xs="auto">
                        <Button size="sm" variant="success" href="/snapshots/new">New Snapshot</Button>
                    </Col>
                    <Col>
                    </Col>
                    <Col xs="auto">
                        <Button size="sm" variant="primary"><FontAwesomeIcon icon={faSync} /></Button>
                    </Col>
                </Row>
            </div>

            <MyTable data={sources} columns={columns} />
        </div>;
    }
}
