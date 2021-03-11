import { faUserFriends } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Dropdown from 'react-bootstrap/Dropdown';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import { handleChange } from './forms';
import { PolicyEditor } from './PolicyEditor';
import MyTable from './Table';
import { compare, DirectorySelector, isAbsolutePath, ownerName, redirectIfNotConnected } from './uiutil';

const localPolicies = "Local Policies"
const allPolicies = "All Policies"
const globalPolicy = "Global Policy"
const perUserPolicies = "Per-User Policies"
const perHostPolicies = "Per-Host Policies"
export class PoliciesTable extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: false,
            error: null,
            editorTarget: null,
            selectedOwner: localPolicies,
            policyPath: "",
            sources: [],
        };

        this.editorClosed = this.editorClosed.bind(this);
        this.editPolicyForPath = this.editPolicyForPath.bind(this);
        this.handleChange = handleChange.bind(this);
        this.fetchPolicies = this.fetchPolicies.bind(this);
        this.fetchSourcesWithoutSpinner = this.fetchSourcesWithoutSpinner.bind(this);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchPolicies();
        this.fetchSourcesWithoutSpinner();
    }

    sync() {
        this.fetchPolicies();

        axios.post('/api/v1/repo/sync', {}).then(result => {
            this.fetchSourcesWithoutSpinner();
        }).catch(error => {
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    fetchPolicies() {
        axios.get('/api/v1/policies').then(result => {
            this.setState({
                items: result.data.policies,
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

    fetchSourcesWithoutSpinner() {
        axios.get('/api/v1/sources').then(result => {
            this.setState({
                localSourceName: result.data.localUsername + "@" + result.data.localHost,
                localUsername: result.data.localUsername,
                localHost: result.data.localHost,
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

    editorClosed() {
        this.setState({
            editorTarget: null,
        });
        this.fetchPolicies();
    }

    editPolicyForPath(e) {
        e.preventDefault();

        if (!this.state.policyPath) {
            return;
        }

        if (!isAbsolutePath(this.state.policyPath)) {
            alert("Policies can only be defined for absolute paths.");
            return;
        }

        this.setState({
            editorTarget: {
                userName: this.state.localUsername,
                host: this.state.localHost,
                path: this.state.policyPath,
            },
        })
    }

    selectOwner(h) {
        this.setState({
            selectedOwner: h,
        });
    }

    policySummary(p) {
        function isEmpty(obj) {
            for (var key in obj) {
                if (obj.hasOwnProperty(key))
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
        let { items, sources, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }
        if (isLoading) {
            return <p>Loading ...</p>;
        }


        let uniqueOwners = sources.reduce((a, d) => {
            const owner = ownerName(d.source);

            if (!a.includes(owner)) { a.push(owner); }
            return a;
        }, []);

        uniqueOwners.sort();

        switch (this.state.selectedOwner) {
            case allPolicies:
                // do nothing;
                break;

            case globalPolicy:
                items = items.filter(x => !x.target.userName && !x.target.host && !x.target.path);
                break;

            case localPolicies:
                items = items.filter(x => ownerName(x.target) === this.state.localSourceName && x.target.path.startsWith(this.state.policyPath));
                break;

            case perUserPolicies:
                items = items.filter(x => !!x.target.userName && !!x.target.host && !x.target.path);
                break;

            case perHostPolicies:
                items = items.filter(x => !x.target.userName && !!x.target.host && !x.target.path);
                break;

            default:
                items = items.filter(x => ownerName(x.target) === this.state.selectedOwner);
                break;
        };

        items.sort((l,r) => {
            const hc = compare(l.target.host,r.target.host);
            if (hc) {
                return hc;
            }
            const uc = compare(l.target.userName,r.target.userName);
            if (uc) {
                return uc;
            }
            return compare(l.target.path,r.target.path);
        });


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
            {!this.state.editorTarget && <div className="list-actions">
                <Form onSubmit={this.editPolicyForPath}>
                    <Row>
                        <Col xs="auto">
                            <Dropdown>
                                <Dropdown.Toggle size="sm" variant="outline-primary" id="dropdown-basic">
                                    <FontAwesomeIcon icon={faUserFriends} />&nbsp;{this.state.selectedOwner}
                                </Dropdown.Toggle>

                                <Dropdown.Menu>
                                    <Dropdown.Item onClick={() => this.selectOwner(localPolicies)}>{localPolicies}</Dropdown.Item>
                                    <Dropdown.Item onClick={() => this.selectOwner(allPolicies)}>{allPolicies}</Dropdown.Item>
                                    <Dropdown.Divider />
                                    <Dropdown.Item onClick={() => this.selectOwner(globalPolicy)}>{globalPolicy}</Dropdown.Item>
                                    <Dropdown.Item onClick={() => this.selectOwner(perUserPolicies)}>{perUserPolicies}</Dropdown.Item>
                                    <Dropdown.Item onClick={() => this.selectOwner(perHostPolicies)}>{perHostPolicies}</Dropdown.Item>
                                    <Dropdown.Divider />
                                    {uniqueOwners.map(v => <Dropdown.Item key={v} onClick={() => this.selectOwner(v)}>{v}</Dropdown.Item>)}
                                </Dropdown.Menu>
                            </Dropdown>
                        </Col>
                        {this.state.selectedOwner === localPolicies ? <>
                            <Col>
                                <DirectorySelector autoFocus onDirectorySelected={p => this.setState({ policyPath: p })} 
                                placeholder="enter directory to find or set policy"
                                name="policyPath" value={this.state.policyPath} onChange={this.handleChange} />
                            </Col>
                            <Col xs="auto">
                                <Button disabled={!this.state.policyPath} size="sm" type="submit" onClick={this.editPolicyForPath}>Set Policy</Button>
                            </Col>
                        </> : <Col />}
                    </Row>
                </Form>
            </div>}

            {items.length > 0 ? <div className={this.state.editorTarget ? "hidden" : "normal"}>
                <p>Found {items.length} policies matching criteria.</p>
                <MyTable data={items} columns={columns} />
            </div> : ((this.state.selectedOwner === localPolicies && this.state.policyPath) ? <p>
                No policy found for directory <code>{this.state.policyPath}</code>. Click <b>Set Policy</b> to define it.
            </p> : <p>No policies found.</p>)}

            {this.state.editorTarget && <PolicyEditor host={this.state.editorTarget.host} userName={this.state.editorTarget.userName} path={this.state.editorTarget.path} close={this.editorClosed} />}
        </div>;
    }
}
