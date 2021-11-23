import { faCalendarTimes, faChevronLeft, faClock, faExclamationTriangle, faFolderOpen, faMagic, faPuzzlePiece } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import Accordion from 'react-bootstrap/Accordion';
import { handleChange, OptionalBoolean, OptionalNumberField, RequiredBoolean, stateProperty, StringList, valueToNumber } from './forms';
import { errorAlert, sourceQueryStringParams } from './uiutil';

function PolicyTypeName(s) {
    if (!s.host && !s.userName) {
        return "Global Policy"
    }

    if (!s.userName) {
        return "Host: " + s.host;
    }

    if (!s.path) {
        return "User: " + s.userName + "@" + s.host;
    }

    return "Directory: " + s.userName + "@" + s.host + ":" + s.path;
}

function LabelColumn(props) {
    return <Col xs={12} sm={4} className="policyFieldColumn">
        <span class="policyField">{props.name}</span>
        {props.help && <><p className="label-help">{props.help}</p></>}
    </Col>
}

function ValueColumn(props) {
    return <Col xs={12} sm={4} className="policyValue">{props.children}</Col>;
}

function WideValueColumn(props) {
    return <Col xs={12} sm={4} className="policyValue">{props.children}</Col>;
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

        this.setState({ saving: true });
        axios.put(this.snapshotURL(this.props), policy).then(result => {
            this.props.close();
        }).catch(error => {
            this.setState({ saving: false });
            errorAlert(error, 'Error saving policy');
        });
    }

    deletePolicy() {
        if (window.confirm('Are you sure you want to delete this policy?')) {
            this.setState({ saving: true });

            axios.delete(this.snapshotURL(this.props)).then(result => {
                this.props.close();
            }).catch(error => {
                this.setState({ saving: false });
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
                &nbsp;&nbsp;{PolicyTypeName(this.props)}</h4>}
            <Form onSubmit={this.saveChanges}>

                <Accordion defaultActiveKey="scheduling">
                    <Accordion.Item eventKey="retention">
                        <Accordion.Header><FontAwesomeIcon icon={faCalendarTimes} />&nbsp;Snapshot Retention</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Latest Snapshots" help="Number of the most recent snapshots to retain per source." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepLatest", { placeholder: "# of latest snapshots" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Hourly" help="How many hourly snapshots to retain per source. The latest snapshot from each hour will be retained." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepHourly", { placeholder: "# of hourly snapshots" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Daily" help="How many daily snapshots to retain per source. The latest snapshot from each day will be retained." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepDaily", { placeholder: "# of daily snapshots" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Weekly" help="How many weekly snapshots to retain per source. The latest snapshot from each week will be retained." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepWeekly", { placeholder: "# of weekly snapshots" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Monthly" help="How many monthly snapshots to retain per source. The latest snapshot from each calendar month will be retained." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepMonthly", { placeholder: "# of monthly snapshots" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Annual" help="How many annual snapshots to retain per source. The latest snapshot from each calendar year will be retained." />
                                <ValueColumn>{OptionalNumberField(this, null, "policy.retention.keepAnnual", { placeholder: "# of annual snapshots" })}</ValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                    <Accordion.Item eventKey="files">
                        <Accordion.Header><FontAwesomeIcon icon={faFolderOpen} />&nbsp;Files</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Ignore Files" help="List of file and directory names to ignore. The patterns should be specified as relative to the directory they are defined in and not absolute. Wilcards are allowed." />
                                <WideValueColumn>{StringList(this, "policy.files.ignore")}</WideValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore Rules From Parent Directories" help="When set, ignore rules from the parent directory are ignored." />
                                <ValueColumn>
                                    {RequiredBoolean(this, "", "policy.files.noParentIgnore")}
                                </ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore Rule Files" help="List of additional files containing ignore rules. Each file configures ignore rules for the directory and its subdirectories." />
                                <ValueColumn>{StringList(this, "policy.files.ignoreDotFiles")}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore Rule Files From Parent Directories" help="When set, the files specifying ignore rules (.kopiaignore, etc.) from the parent directory are ignored." />
                                <ValueColumn>{RequiredBoolean(this, "", "policy.files.noParentDotFiles")}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore Well-Known Cache Directories" help="Ignore directories containing CACHEDIR.TAG and similar." />
                                <ValueColumn>{OptionalBoolean(this, null, "policy.files.ignoreCacheDirs", "inherit from parent")}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Scan only one filesystem" help="Do not cross filesystem boundaries when snapshotting." />
                                <ValueColumn>{OptionalBoolean(this, null, "policy.files.oneFileSystem", "inherit from parent")}</ValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                    <Accordion.Item eventKey="errors">
                        <Accordion.Header><FontAwesomeIcon icon={faExclamationTriangle} />&nbsp;Error Handling</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Ignore Directory Errors" help="Treat directory read errors as non-fatal." />
                                <ValueColumn>{OptionalBoolean(this, null, "policy.errorHandling.ignoreDirectoryErrors", "inherit from parent")}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore File Errors" help="Treat file read errors as non-fatal." />
                                <ValueColumn>{OptionalBoolean(this, null, "policy.errorHandling.ignoreFileErrors", "inherit from parent")}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Ignore Unknown Directory Entries" help="Treat unrecognized/unsupported directory entries as non-fatal errors." />
                                <ValueColumn>{OptionalBoolean(this, null, "policy.errorHandling.ignoreUnknownTypes", "inherit from parent")}</ValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                    <Accordion.Item eventKey="compression">
                        <Accordion.Header><FontAwesomeIcon icon={faPuzzlePiece} />&nbsp;Compression</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Compression Algorithm" help="Specify compression algorithm to use when snapshotting files in this directory and subdirectories." />
                                <WideValueColumn>
                                    <Form.Control as="select"
                                        name="policy.compression.compressorName"
                                        onChange={this.handleChange}
                                        value={stateProperty(this, "policy.compression.compressorName")}>
                                        <option value="">(none)</option>
                                        {this.state.algorithms && this.state.algorithms.compression.map(x => <option key={x} value={x}>{x}</option>)}
                                    </Form.Control>
                                </WideValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Minimum File Size" help="Files whose size is below the provided value will not be compressed." />
                                <ValueColumn>{OptionalNumberField(this, "", "policy.compression.minSize", { placeholder: "minimum file size in bytes" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Max File Size" help="Files whose size exceeds the provided value will not be compressed." />
                                <ValueColumn>{OptionalNumberField(this, "", "policy.compression.maxSize", { placeholder: "maximum file size in bytes" })}</ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Only Compress Extensions" help="Only compress files with the following file extensions (one extension per line)" />
                                <WideValueColumn>
                                    {StringList(this, "policy.compression.onlyCompress")}
                                </WideValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Never Compress Extensions" help="Never compress the following file extensions (one extension per line)" />
                                <WideValueColumn>
                                    {StringList(this, "policy.compression.neverCompress")}
                                </WideValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                    <Accordion.Item eventKey="scheduling">
                        <Accordion.Header><FontAwesomeIcon icon={faClock} />&nbsp;Scheduling</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Snapshot Frequency" help="How frequently to create snapshots in KopiaUI or kopia server. This option has no effect outside of the server mode." />
                                <WideValueColumn>
                                    <Form.Control as="select"
                                        name="policy.scheduling.intervalSeconds"
                                        onChange={e => this.handleChange(e, valueToNumber)}
                                        value={stateProperty(this, "policy.scheduling.intervalSeconds")}>
                                        <option value="">(none)</option>
                                        <option value="600">every 10 minutes</option>
                                        <option value="900">every 15 minutes</option>
                                        <option value="1200">every 20 minutes</option>
                                        <option value="1800">every 30 minutes</option>
                                        <option value="3600">every hour</option>
                                        <option value="10800">every 3 hours</option>
                                        <option value="21600">every 6 hours</option>
                                        <option value="43200">every 12 hours</option>
                                    </Form.Control>
                                </WideValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="Manual Snapshots Only" help="Only create snapshots manually (disables scheduled snapshots)." />
                                <ValueColumn>
                                    {OptionalBoolean(this, "", "policy.scheduling.manual", "inherit from parent")}
                                </ValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                    <Accordion.Item eventKey="other">
                        <Accordion.Header><FontAwesomeIcon icon={faMagic} />&nbsp;Other</Accordion.Header>
                        <Accordion.Body>
                            <Row>
                                <LabelColumn name="Disable Parent Policy Evaluation" help="prevents any parent policies from affecting this directory and subdirectories" />
                                <ValueColumn>
                                    {RequiredBoolean(this, "", "policy.noParent")}
                                </ValueColumn>
                            </Row>
                            <Row>
                                <LabelColumn name="JSON Representation" help="This is the internal representation of a policy." />
                                <WideValueColumn>
                                    <pre className="debug-json">{JSON.stringify(this.state.policy, null, 4)}
                                    </pre>
                                </WideValueColumn>
                            </Row>
                        </Accordion.Body>
                    </Accordion.Item>
                </Accordion>

                <Button size="sm" variant="success" type="submit" onClick={this.saveChanges} disabled={this.state.saving}>Save Policy</Button>
                {!this.state.isNew && <>&nbsp;
                    <Button size="sm" variant="danger" disabled={this.isGlobal() || this.state.saving} onClick={this.deletePolicy}>Delete Policy</Button>
                </>}
                {this.state.saving && <>
                    &nbsp;
                    <Spinner animation="border" variant="primary" size="sm" />
                </>}

            </Form>
        </div>;
    }
}
