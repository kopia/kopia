import axios from 'axios';
import React, { Component } from 'react';
import Badge from 'react-bootstrap/Badge';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Spinner from 'react-bootstrap/Spinner';
import { Link } from "react-router-dom";
import MyTable from './Table';
import { CLIEquivalent, compare, GoBackButton, objectLink, parseQuery, rfc3339TimestampForDisplay, sizeWithFailures, sourceQueryStringParams } from './uiutil';

function pillVariant(tag) {
    if (tag.startsWith("latest-")) {
        return "success";
    }
    if (tag.startsWith("daily-")) {
        return "info";
    }
    if (tag.startsWith("weekly-")) {
        return "danger";
    }
    if (tag.startsWith("monthly-")) {
        return "dark";
    }
    if (tag.startsWith("annual-")) {
        return "warning";
    }
    return "primary";
}

export class SnapshotsTable extends Component {
    constructor() {
        super();
        this.state = {
            snapshots: [],
            showHidden: false,
            isLoading: false,
            error: null,
        };
        this.onChange = this.onChange.bind(this);
    }

    componentDidUpdate(oldProps, oldState) {
        if (this.state.showHidden !== oldState.showHidden) {
            this.fetchSnapshots();
        }
    }

    componentDidMount() {
        this.fetchSnapshots();
    }

    fetchSnapshots() {
        let q = parseQuery(this.props.location.search);

        this.setState({
            isLoading: true,
            host: q.host,
            userName: q.userName,
            path: q.path,
            hiddenCount: 0,
            selectedSnapshot: null,
        });

        let u = '/api/v1/snapshots?' + sourceQueryStringParams(q);

        if (this.state.showHidden) {
            u += "&all=1";
        }

        axios.get(u).then(result => {
            console.log('got snapshots', result.data);
            this.setState({
                snapshots: result.data.snapshots,
                unfilteredCount: result.data.unfilteredCount,
                uniqueCount: result.data.uniqueCount,
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    selectSnapshot(x) {
        this.setState({
            selectedSnapshot: x,
        })
    }

    onChange(x) {
        this.setState({
            showHidden: x.target.checked,
        });
    }

    render() {
        let { snapshots, unfilteredCount, uniqueCount, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }

        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        snapshots.sort((a, b) => -compare(a.startTime, b.startTime));

        const columns = [{
            id: 'startTime',
            Header: 'Start time',
            width: 200,
            accessor: x => <Link to={objectLink(x.rootID)}>{rfc3339TimestampForDisplay(x.startTime)}</Link>,
        }, {
            id: 'rootID',
            Header: 'Root',
            width: "",
            accessor: x => x.rootID,
        }, {
            Header: 'Retention',
            accessor: 'retention',
            width: "",
            Cell: x => <span>{x.cell.value.map(l =>
                <><Badge bg={pillVariant(l)}>{l}</Badge>{' '}</>
            )}</span>
        }, {
            Header: 'Size',
            accessor: 'summary.size',
            width: 100,
            Cell: x => sizeWithFailures(x.cell.value, x.row.original.summary),
        }, {
            Header: 'Files',
            accessor: 'summary.files',
            width: 100,
        }, {
            Header: 'Dirs',
            accessor: 'summary.dirs',
            width: 100,
        }]

        return <div className="padded">
            <Row>
                <Col>
                    <GoBackButton onClick={this.props.history.goBack} />
                    &nbsp;
                    Displaying {snapshots.length !== unfilteredCount ? snapshots.length + ' out of ' + unfilteredCount : snapshots.length} snapshots of&nbsp;<b>{this.state.userName}@{this.state.host}:{this.state.path}</b>
                    {unfilteredCount !== uniqueCount &&
                        <>&nbsp;<Form.Group controlId="formBasicCheckbox">
                            <Form.Check
                                type="checkbox"
                                checked={this.state.showHidden}
                                label={'Show ' + unfilteredCount + ' individual snapshots'}
                                onChange={this.onChange} />
                        </Form.Group></>}
                </Col>
            </Row>
            <Row>
                <Col xs={12}>
                    <MyTable data={snapshots} columns={columns} />
                </Col>
            </Row>

            <CLIEquivalent command={`snapshot list "${this.state.userName}@${this.state.host}:${this.state.path}"${this.state.showHidden ? " --show-identical" : ""}`} />
        </div>;
    }
}
