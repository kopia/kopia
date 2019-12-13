import React, { Component } from 'react';
import ReactTable from 'react-table';
import axios from 'axios';

import {
    sourceDisplayName,
    intervalDisplayName,
    timesOfDayDisplayName,
} from './uiutil';

export class PoliciesTable extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: false,
            error: null,
        };
    }
    ;
    componentDidMount() {
        axios.get('/api/v1/policies').then(result => {
            this.setState({ "items": result.data.policies });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }
    render() {
        const { items, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <p>Loading ...</p>;
        }
        const columns = [{
            id: 'target',
            Header: 'Target',
            accessor: x => sourceDisplayName(x.target),
        }, {
            Header: 'Latest',
            accessor: 'policy.retention.keepLatest'
        }, {
            Header: 'Hourly',
            accessor: 'policy.retention.keepHourly'
        }, {
            Header: 'Daily',
            accessor: 'policy.retention.keepDaily'
        }, {
            Header: 'Weekly',
            accessor: 'policy.retention.keepWeekly'
        }, {
            Header: 'Monthly',
            accessor: 'policy.retention.keepMonthly'
        }, {
            Header: 'Annual',
            accessor: 'policy.retention.keepAnnual'
        }, {
            id: 'interval',
            Header: 'Interval',
            accessor: x => intervalDisplayName(x.policy.scheduling.interval),
        }, {
            id: 'timesOfDay',
            Header: 'Times of Day',
            accessor: x => timesOfDayDisplayName(x.policy.scheduling.timesOfDay),
        }]
        
        return <ReactTable data={items} columns={columns} />;
    }
}
