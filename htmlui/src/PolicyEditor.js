import { faChevronLeft } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import { handleChange, OptionalBoolean, OptionalNumberField, RequiredBoolean, stateProperty, StringList } from './forms';
import { errorAlert, sourceQueryStringParams } from './uiutil';

function policyTypeName(s) {
    if (!s.host && !s.userName) {
        return "Global Policy"
    }

    if (!s.userName) {
        return "Per-Host Policy";
    }

    if (!s.path) {
        return "Per-User Policy";
    }

    return "Directory Policy";
}

export class PolicyEditor extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: false,
            error: null,
        };

        this.fetchPolicy = this.fetchPolicy.bind(this);
        this.handleChange = handleChange.bind(this);
        this.saveChanges = this.saveChanges.bind(this);
        this.isGlobal = this.isGlobal.bind(this);
        this.deletePolicy = this.deletePolicy.bind(this);
        this.snapshotURL = this.snapshotURL.bind(this);
    }

    componentDidMount() {
        axios.get('/api/v1/repo/algorithms').then(result => {
            this.setState({
                algorithms: result.data,
            });

            this.fetchPolicy(this.props);
        });
    }

    componentWillReceiveProps(props) {
        this.fetchPolicy(props);
    }

    fetchPolicy(props) {
        axios.get(this.snapshotURL(props)).then(result => {
            this.setState({
                isLoading: false,
                policy: result.data,
            });
        }).catch(error => {
            if (error.response && error.response.data.code !== "NOT_FOUND") {
                this.setState({
                    error: error,
                    isLoading: false
                })
            } else {
                this.setState({
                    policy: {},
                    isNew: true,
                    isLoading: false
                })
            }
        });
    }

    saveChanges(e) {
        e.preventDefault()

        function removeEmpty(l) {
            if (!l) {
                return l;
            }

            let result = [];
            for (let i = 0; i < l.length; i++) {
                const s = l[i];
                if (s === "") {
                    continue;
                }

                result.push(s);
            }

            return result;
        }

        // clean up policy before saving
        let policy = JSON.parse(JSON.stringify(this.state.policy));
        if (policy.files) {
            if (policy.files.ignore) {
                policy.files.ignore = removeEmpty(policy.files.ignore)
            }
            if (policy.files.ignoreDotFiles) {
                policy.files.ignoreDotFiles = removeEmpty(policy.files.ignoreDotFiles)
            }
        }

        if (policy.compression) {
            if (policy.compression.onlyCompress) {
                policy.compression.onlyCompress = removeEmpty(policy.compression.onlyCompress)
            }
            if (policy.compression.neverCompress) {
                policy.compression.neverCompress = removeEmpty(policy.compression.neverCompress)
            }
        }

        this.setState({saving: true});
        axios.put(this.snapshotURL(this.props), policy).then(result => {
            this.props.close();
        }).catch(error => {
            this.setState({saving: false});
            errorAlert(error, 'Error saving policy');
        });
    }

    deletePolicy() {
        if (window.confirm('Are you sure you want to delete this policy?')) {
            this.setState({saving: true});

            axios.delete(this.snapshotURL(this.props)).then(result => {
                this.props.close();
            }).catch(error => {
                this.setState({saving: false});
                errorAlert(error, 'Error deleting policy');
            });
        }
    }

    snapshotURL(props) {
        return '/api/v1/policy?' + sourceQueryStringParams(props);
    }

    isGlobal() {
        return !this.props.host && !this.props.userName && !this.props.path;
    }

    render() {
        const { isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }

        if (isLoading) {
            return <p>Loading ...</p>;
        }

        return <div className="padded">
            {!this.props.embedded && <h4><Button size="sm" variant="outline-secondary" onClick={this.props.close} ><FontAwesomeIcon icon={faChevronLeft} /> Return </Button>
            &nbsp;&nbsp;{policyTypeName(this.props)}</h4>}
            <Form onSubmit={this.saveChanges}>
            {!this.props.embedded && <Row>
                <Col xs="2">
                    <Form.Group>
                        <Form.Label className="required">Target User</Form.Label>
                        <Form.Control size="sm" type="text" readOnly value={this.props.userName || "<any user>"} />
                    </Form.Group>
                </Col>
                <Col xs="2">
                    <Form.Group>
                        <Form.Label className="required">Target Host</Form.Label>
                        <Form.Control size="sm" type="text" readOnly value={this.props.host || "<any host>"} />
                    </Form.Group>
                </Col>
                <Col xs="8">
                    <Form.Group>
                        <Form.Label className="required">Target Directory Path</Form.Label>
                        <Form.Control size="sm" type="text" readOnly value={this.props.path || "<any path>"} />
                    </Form.Group>
                </Col>
            </Row>}
            <Tabs defaultActiveKey="retention">
                <Tab eventKey="retention" title="Retention">
                    <div className="tab-body">
                        <p className="policy-help">Controls how many latest snapshots to keep per source directory</p>
                        <Form.Row>
                            {OptionalNumberField(this, "Latest", "policy.retention.keepLatest", { placeholder: "# of latest snapshots" })}
                            {OptionalNumberField(this, "Hourly", "policy.retention.keepHourly", { placeholder: "# of hourly snapshots" })}
                            {OptionalNumberField(this, "Daily", "policy.retention.keepDaily", { placeholder: "# of daily snapshots" })}
                        </Form.Row>
                        <Form.Row>
                            {OptionalNumberField(this, "Weekly", "policy.retention.keepWeekly", { placeholder: "# of weekly snapshots" })}
                            {OptionalNumberField(this, "Monthly", "policy.retention.keepMonthly", { placeholder: "# of monthly snapshots" })}
                            {OptionalNumberField(this, "Annual", "policy.retention.keepAnnual", { placeholder: "# of annual snapshots" })}
                        </Form.Row>
                    </div>
                </Tab>
                <Tab eventKey="files" title="Files">
                    <div className="tab-body">
                        <p className="policy-help">Controls which files should be included and excluded when snapshotting. Use <a target="_blank" rel="noreferrer" href="https://git-scm.com/docs/gitignore">.gitignore</a> syntax.</p>
                        <Form.Row>
                            {StringList(this, "Ignore Rules", "policy.files.ignore", "List of file name patterns to ignore.")}
                            {StringList(this, "Ignore Rule Files", "policy.files.ignoreDotFiles", "List of additional files containing ignore rules. Each file configures ignore rules for the directory and its subdirectories.")}
                        </Form.Row>
                        <Form.Row>
                            {RequiredBoolean(this, "Ignore Parent Rules", "policy.files.noParentIgnore")}
                            {RequiredBoolean(this, "Ignore Parent Rule Files", "policy.files.noParentDotFiles")}
                        </Form.Row>
                        <Form.Row>
                            {OptionalBoolean(this, "Ignore Well-Known Cache Directories", "policy.files.ignoreCacheDirs", "inherit from parent")}
                        </Form.Row>
                        <Form.Row>
                            {OptionalBoolean(this, "Scan only one filesystem", "policy.files.oneFileSystem", "inherit from parent")}
                        </Form.Row>
                    </div>
                </Tab>
                <Tab eventKey="errors" title="Errors">
                    <div className="tab-body">
                        <p className="policy-help">Controls how errors detected while snapshotting are handled.</p>
                        <Form.Row>
                            {OptionalBoolean(this, "Ignore Directory Errors", "policy.errorHandling.ignoreDirectoryErrors", "inherit from parent")}
                            {OptionalBoolean(this, "Ignore File Errors", "policy.errorHandling.ignoreFileErrors", "inherit from parent")}
                            {OptionalBoolean(this, "Ignore Unknown Types", "policy.errorHandling.ignoreUnknownTypes", "inherit from parent")}
                        </Form.Row>
                    </div>
                </Tab>
                <Tab eventKey="compression" title="Compression">
                    <div className="tab-body">
                        <p className="policy-help">Controls which files are compressed.</p>
                        <Form.Row>
                            <Form.Group as={Col}>
                                <Form.Label className="required">Compression</Form.Label>
                                <Form.Control as="select"
                                    name="policy.compression.compressorName"
                                    onChange={this.handleChange}
                                    value={stateProperty(this, "policy.compression.compressorName")}>
                                    <option value="">(none)</option>
                                    {this.state.algorithms && this.state.algorithms.compression.map(x => <option key={x} value={x}>{x}</option>)}
                                </Form.Control>
                            </Form.Group>
                            {OptionalNumberField(this, "Min File Size", "policy.compression.minSize", { placeholder: "minimum file size to compress" })}
                            {OptionalNumberField(this, "Max File Size", "policy.compression.maxSize", { placeholder: "maximum file size to compress" })}
                        </Form.Row>
                        <Form.Row>
                            {StringList(this, "Only Compress Extensions", "policy.compression.onlyCompress", "Only compress files with the above file extensions (one extension per line)")}
                            {StringList(this, "Never Compress Extensions", "policy.compression.neverCompress", "Never compress the above file extensions (one extension per line)")}
                        </Form.Row>
                    </div>
                </Tab>
                <Tab eventKey="scheduling" title="Scheduling">
                    <div className="tab-body">
                        <p className="policy-help">Controls when snapshots are automatically created.</p>
                        <Form.Row>
                            {OptionalNumberField(this, "Snapshot Interval", "policy.scheduling.intervalSeconds", { placeholder: "seconds" })}
                        </Form.Row>
                        <Form.Row>
                            {OptionalBoolean(this, "Only create snapshots manually (disables scheduled snapshots)", "policy.scheduling.manual")}
                        </Form.Row>
                    </div>
                </Tab>
            </Tabs>
            <Form.Row>
                {RequiredBoolean(this, "Disable Parent Policy Evaluation (prevents any parent policies from affecting this directory and subdirectories)", "policy.noParent")}
            </Form.Row>

            <Button size="sm" variant="success" type="submit" onClick={this.saveChanges} disabled={this.state.saving}>Save Policy</Button>
            {!this.state.isNew && <>&nbsp;
            <Button size="sm" variant="danger" disabled={this.isGlobal() || this.state.saving} onClick={this.deletePolicy}>Delete Policy</Button>
            </>}
            {this.state.saving && <>
                &nbsp;
                <Spinner animation="border" variant="primary" size="sm" />
                </>}

            {!this.props.embedded && <>
            <hr />
            <h5>JSON representation</h5>
            <pre className="debug-json">{JSON.stringify(this.state.policy, null, 4)}
            </pre>
            </>}
            </Form>
        </div>;
    }
}
