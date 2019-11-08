import React, { Component } from 'react';
import ReactTable from 'react-table';
import axios from 'axios';

import {
    parseQuery,
    sizeDisplayName,
    objectLink,
    rfc3339TimestampDisplayName,
} from './uiutil';

import {
    Link
} from "react-router-dom";

import Spinner from 'react-bootstrap/Spinner';
import Badge from 'react-bootstrap/Badge';
import Form from 'react-bootstrap/Form';


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
            isLoading: false,
            error: null,
        };
    }
    componentDidMount() {
        let q = parseQuery(this.props.location.search);

        this.setState({ 
            isLoading: true,
            host: q.host,
            userName: q.userName,
            path: q.path,
            hiddenCount: 0,
            selectedSnapshot: null,
         });
        const u = '/api/v1/snapshots?host=' + q.host + '&userName=' + q.userName + '&path=' + q.path;
        console.log('u', u);
        axios.get(u).then(result => {
            console.log('got snapshots', result.data);
            this.setState({
                snapshots: result.data.snapshots,
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    coalesceSnapshots(s) {
        let filteredSnapshots = [];

        let lastRootID = "";
        let hiddenCount = 0;

        for (let i = 0; i < s.length; i++) {
            if (s[i].rootID !== lastRootID) {
                filteredSnapshots.push(s[i]);
            } else {
                hiddenCount++;
            }
            lastRootID = s[i].rootID;
        }
        return { filteredSnapshots, hiddenCount };
    }

    selectSnapshot(x) {
        this.setState({
            selectedSnapshot: x,
        })
    }

    render() {
        let { snapshots, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        snapshots.sort((a,b)=> {
            if (a.startTime < b.startTime) { return 1; };
            if (a.startTime > b.startTime) { return -1; };
            return 0;
        });

        let { filteredSnapshots, hiddenCount } = this.coalesceSnapshots(snapshots);

        const columns = [{
            Header: 'Start time',
            accessor: 'startTime',
            width: 200,
            Cell: x => rfc3339TimestampDisplayName(x.value),
        }, {
            Header: 'Size',
            accessor: 'summary.size',
            width: 100,
            Cell: x => sizeDisplayName(x.value),
        }, {
            Header: 'Files',
            accessor: 'summary.files',
            width: 100,
        }, {
            Header: 'Dirs',
            accessor: 'summary.dirs',
            width: 100,
        }, {
            id: 'rootID',
            Header: 'Root',
            accessor: x => <Link to={objectLink(x.rootID)}>{x.rootID}</Link>,
            width: 300,
        }, {
            Header: 'Retention',
            accessor: 'retention',
            Cell: x => <span>{x.value.map(l => 
                <Badge variant={pillVariant(l)}>{l}</Badge>
            )}</span>
        }]

        return <div>
            <Form>
            <Form.Group controlId="formBasicCheckbox">
                <Form.Label>Displaying {filteredSnapshots.length} snapshots of <b>{this.state.userName}@{this.state.host}:{this.state.path}</b></Form.Label>
            </Form.Group>
                
  <Form.Group controlId="formBasicCheckbox">
    <Form.Check type="checkbox" label={'Show ' + hiddenCount + ' hidden snapshots'} />
  </Form.Group>

  {this.state.selectedSnapshot ? 
    <Form.Group controlId="formSelected">
    <Form.Label>Selected <pre>{JSON.stringify(this.state.selectedSnapshot, null, 2)}</pre></Form.Label>
  </Form.Group> : null}
</Form>
            <ReactTable data={filteredSnapshots} columns={columns} />
            </div>;
    }
}
