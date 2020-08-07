import axios from 'axios';
import moment from 'moment';
import React, { Component } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import ButtonGroup from 'react-bootstrap/ButtonGroup';
import ButtonToolbar from 'react-bootstrap/ButtonToolbar';
import Dropdown from 'react-bootstrap/Dropdown';
import DropdownButton from 'react-bootstrap/DropdownButton';
import FormControl from 'react-bootstrap/FormControl';
import InputGroup from 'react-bootstrap/InputGroup';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import { Link } from 'react-router-dom';
import { handleChange } from './forms';
import MyTable from './Table';
import { compare, ownerName, redirectIfNotConnected, sizeDisplayName, sizeWithFailures } from './uiutil';

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
            selectedOwner: localSnapshots,
            selectedDirectory: "",
        };

        this.sync = this.sync.bind(this);
        this.fetchSourcesWithoutSpinner = this.fetchSourcesWithoutSpinner.bind(this);
        this.selectDirectory = this.selectDirectory.bind(this);
        this.handleChange = handleChange.bind(this);

        this.snapshotOnce = this.snapshotOnce.bind(this);
        this.snapshotEveryDay = this.snapshotEveryDay.bind(this);
        this.snapshotEveryHour = this.snapshotEveryHour.bind(this);
        this.createPolicy = this.createPolicy.bind(this);
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
            alert('failed');
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    selectDirectory() {
        // populated in 'preload.js' in Electron
        if (!window.require) {
            alert('Directory selection is not supported in a web browser.\n\nPlease enter path manually.');
            return;
        }

        const { dialog } = window.require('electron').remote;
        try {
            let dir = dialog.showOpenDialogSync({
                properties: ['openDirectory']
            });
            if (dir) {
                this.setState({
                    selectedDirectory: dir[0],
                });
            }
        } catch (e) {
            window.alert('Error: ' + e);
        }
    }

    createSource(request) {
        if (!this.state.selectedDirectory) {
            alert('Must specify directory to snapshot.');
            return
        }

        axios.post('/api/v1/sources', request).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            if (error.response) {
                alert('Error: ' + error.response.data.error + " (" + error.response.data.code + ")");
                return
            }

            this.setState({
                error,
                isLoading: false
            });
        });
    }

    snapshotOnce() {
        this.createSource({
            path: this.state.selectedDirectory,
            createSnapshot: true,
            initialPolicy: {
            }
        });
    }

    snapshotEveryDay() {
        this.createSource({
            path: this.state.selectedDirectory,
            createSnapshot: true,
            initialPolicy: {
                scheduling: { intervalSeconds: 86400 },
            }
        });
    }

    snapshotEveryHour() {
        this.createSource({
            path: this.state.selectedDirectory,
            createSnapshot: true,
            initialPolicy: {
                scheduling: { intervalSeconds: 3600 },
            }
        });
    }

    createPolicy() {
        this.createSource({
            path: this.state.selectedDirectory,
            createSnapshot: false,
            initialPolicy: {
                scheduling: { intervalSeconds: 3600 },
            }
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
                    if (x.row.original.lastSnapshot) {
                        const percent = Math.round(totalBytes * 1000.0 / x.row.original.lastSnapshot.stats.totalSize) / 10.0;
                        if (percent <= 100) {
                            totals += " " + percent + "%";
                        }
                    }
                }

                return <>
                    <Spinner animation="border" variant="primary" size="sm" title={title} />&nbsp;Snapshotting {totals}
                    &nbsp;
                    <Button variant="danger" size="sm" onClick={() => {
                        parent.cancelSnapshot(x.row.original.source);
                    }}>stop</Button>
                </>;

            default:
                return "";
        }
    }

    cancelSnapshot(source) {
        axios.post('/api/v1/sources/cancel?userName=' + source.userName + '&host=' + source.host + '&path=' + source.path, {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            alert('failed');
        });
    }

    startSnapshot(source) {
        axios.post('/api/v1/sources/upload?userName=' + source.userName + '&host=' + source.host + '&path=' + source.path, {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            alert('failed');
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
            Cell: x => <Link to={'/snapshots/single-source?userName=' + x.cell.value.userName + '&host=' + x.cell.value.host + '&path=' + x.cell.value.path}>{x.cell.value.path}</Link>,
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
            <ButtonToolbar className="float-sm-right">
                &nbsp;
                <ButtonGroup>
                    <Dropdown>
                        <Dropdown.Toggle variant="outline-primary" id="dropdown-basic">
                            {this.state.selectedOwner}
                        </Dropdown.Toggle>

                        <Dropdown.Menu>
                            <Dropdown.Item onClick={() => this.selectOwner(localSnapshots)}>{localSnapshots}</Dropdown.Item>
                            <Dropdown.Item onClick={() => this.selectOwner(allSnapshots)}>{allSnapshots}</Dropdown.Item>
                            <Dropdown.Divider />
                            {uniqueOwners.map(v => <Dropdown.Item key={v} onClick={() => this.selectOwner(v)}>{v}</Dropdown.Item>)}
                        </Dropdown.Menu>
                    </Dropdown>
                </ButtonGroup>
                &nbsp;
                <ButtonGroup>
                    <Button variant="primary">Refresh</Button>
                </ButtonGroup>
            </ButtonToolbar>
            <ButtonToolbar>
                <InputGroup>
                    <FormControl
                        id="snapshot-path"
                        placeholder="Enter source path to create new snapshot"
                        name="selectedDirectory"
                        value={this.state.selectedDirectory}
                        onChange={this.handleChange}
                    />
                    <Button as={InputGroup.Prepend}
                        title="Snapshot"
                        variant="primary"
                        id="input-group-dropdown-2"
                        onClick={this.selectDirectory}>...</Button>
                </InputGroup>
                &nbsp;
                <DropdownButton
                    as={InputGroup.Append}
                    variant="success"
                    title="New Snapshot"
                    id="dropdown1">
                    <Dropdown.Item href="#" onClick={this.snapshotOnce}>Snapshot Once</Dropdown.Item>
                    <Dropdown.Item href="#" onClick={this.snapshotEveryHour}>Snapshot Every Hour</Dropdown.Item>
                    <Dropdown.Item href="#" onClick={this.snapshotEveryDay}>Snapshot Every Day</Dropdown.Item>
                    {/* <Dropdown.Item href="#" onClick={this.createPolicy}>Create Policy</Dropdown.Item> */}
                </DropdownButton>
            </ButtonToolbar>
            <hr />
            <Row>
                <MyTable data={sources} columns={columns} />
            </Row>
        </div>;
    }
}
