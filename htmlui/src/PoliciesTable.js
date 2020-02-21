import axios from 'axios';
import React, { Component } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';
import { PolicyEditor } from './PolicyEditor';
import MyTable from './Table';

export class PoliciesTable extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: false,
            error: null,
            editorTarget: null,
            selectedRowIsNew: false,
            targetHost: "*",
            targetUsername: "*",
            targetPath: "*",
        };

        this.editorClosed = this.editorClosed.bind(this);
        this.setNewPolicy = this.setNewPolicy.bind(this);
        this.handleChange = handleChange.bind(this);
        this.fetchPolicies = this.fetchPolicies.bind(this);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchPolicies();
    }

    fetchPolicies() {
        axios.get('/api/v1/policies').then(result => {
            this.setState({
                items: result.data.policies,
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    editorClosed() {
        this.setState({
            editorTarget: null,
        });
        this.fetchPolicies();
    }

    setNewPolicy() {
        if (!validateRequiredFields(this, ["targetHost"])) {
            return;
        }

        function fixInput(a) {
            if (!a) {
                return "";
            }

            // allow both * and empty string to indicate "all"
            if (a === "*") {
                return ""
            }

            return a;
        }
        
        this.setState({
            editorTarget: {
                userName: fixInput(this.state.targetUsername),
                host: this.state.targetHost, 
                path: fixInput(this.state.targetPath),
            },
            selectedRowIsNew: true,
        })
    }

    policySummary(p) {
        function isEmpty(obj) {
            for(var key in obj) {
                if(obj.hasOwnProperty(key))
                    return false;
            }

            return true;
        }

        let bits = [];
        if (!isEmpty(p.policy.retention)) {
            bits.push(<><Badge variant="success">retention</Badge>{' '}</>);
        }
        if (!isEmpty(p.policy.files)) {
            bits.push(<><Badge variant="primary">files</Badge>{' '}</>);
        }
        if (!isEmpty(p.policy.errorHandling)) {
            bits.push(<><Badge variant="danger">errors</Badge>{' '}</>);
        }
        if (!isEmpty(p.policy.compression)) {
            bits.push(<><Badge variant="secondary">compression</Badge>{' '}</>);
        }
        if (!isEmpty(p.policy.scheduling)) {
            bits.push(<><Badge variant="warning">scheduling</Badge>{' '}</>);
        }

        return bits;
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
            Header: 'Username',
            accessor: x => x.target.userName || "*",
        }, {
            Header: 'Host',
            accessor: x => x.target.host || "*",
        }, {
            Header: 'Path',
            accessor: x => x.target.path || "*",
        }, {
            Header: 'Defined',
            width: 300,
            accessor: x => this.policySummary(x),
        }, {
            id: 'edit',
            Header: '',
            width: 50,
            Cell: x => <Button size="sm" onClick={() => {
                this.setState({
                    editorTarget: x.row.original.target,
                    selectedRowIsNew: false,
                })
            }}>Edit</Button>,
        }]

        return <div className="padded">
            <div className={this.state.editorTarget ? "hidden" : "normal"}>
                <MyTable data={items} columns={columns} />
            </div>

            <div className={this.state.editorTarget ? "hidden" : "normal"}>
                <hr/>
            <div className="new-policy-panel"><Form>
                <Form.Row>
                    {OptionalField(this, "Target Username", "targetUsername", {}, "Specify * all empty to target all users")}
                    {RequiredField(this, "Target Host", "targetHost", {})}
                    {OptionalField(this, "Target Path", "targetPath", {}, "Specify * all empty to target all filesystem paths")}
                </Form.Row>
                <Button variant="primary" onClick={this.setNewPolicy}>Set New Policy</Button>
            </Form>
            </div>
            </div>
            {this.state.editorTarget && <PolicyEditor host={this.state.editorTarget.host} userName={this.state.editorTarget.userName} path={this.state.editorTarget.path} close={this.editorClosed} isNew={this.state.selectedRowIsNew} />}
        </div>;
    }
}
