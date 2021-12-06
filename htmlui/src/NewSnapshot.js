import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import { handleChange } from './forms';
import { PolicyEditor } from './PolicyEditor';
import { EstimateResults } from './EstimateResults';
import { CLIEquivalent, DirectorySelector, errorAlert, GoBackButton, redirectIfNotConnected } from './uiutil';

export class NewSnapshot extends Component {
    constructor() {
        super();
        this.state = {
            path: "",
            estimateTaskID: null,
            estimateTaskVisible: false,
            lastEstimatedPath: "",
            policyEditorVisibleFor: "n/a",
            localUsername: null,
        };

        this.policyEditorRef = React.createRef();
        this.handleChange = handleChange.bind(this);
        this.estimate = this.estimate.bind(this);
        this.snapshotNow = this.snapshotNow.bind(this);
    }

    componentDidMount() {
        axios.get('/api/v1/sources').then(result => {
            this.setState({
                localUsername: result.data.localUsername,
                localHost: result.data.localHost,
            });
        }).catch(error => {
            redirectIfNotConnected(error);
        });
    }

    componentDidUpdate() {
        if (this.state.lastResolvedPath !== this.state.path) {
            axios.post('/api/v1/paths/resolve', { path: this.state.path }).then(result => {
                this.setState({
                    lastResolvedPath: this.state.path,
                    resolvedSource: result.data.source,
                });
            }).catch(error => {
                redirectIfNotConnected(error);
            });
        }

        if (this.state.estimateTaskVisible && this.state.lastEstimatedPath !== this.state.resolvedSource.path) {
            this.setState({
                estimateTaskVisible: false,
            })
        }
    }

    estimate(e) {
        e.preventDefault();

        if (!this.state.resolvedSource.path) {
            return;
        }

        const pe = this.policyEditorRef.current;
        if (!pe) {
            return;
        }

        try {
            let req = {
                root: this.state.resolvedSource.path,
                maxExamplesPerBucket: 10,
                policyOverride: pe.getAndValidatePolicy(),
            }

            axios.post('/api/v1/estimate', req).then(result => {
                this.setState({
                    lastEstimatedPath: this.state.resolvedSource.path,
                    estimateTaskID: result.data.id,
                    estimatingPath: result.data.description,
                    estimateTaskVisible: true,
                    didEstimate: false,
                })
            }).catch(error => {
                errorAlert(error);
            });
        } catch (e) {
            errorAlert(e);
        }
    }

    snapshotNow(e) {
        e.preventDefault();

        if (!this.state.resolvedSource.path) {
            alert('Must specify directory to snapshot.');
            return
        }

        const pe = this.policyEditorRef.current;
        if (!pe) {
            return;
        }

        try {
            axios.post('/api/v1/sources', {
                path: this.state.resolvedSource.path,
                createSnapshot: true,
                policy: pe.getAndValidatePolicy(),
            }).then(result => {
                this.props.history.goBack();
            }).catch(error => {
                errorAlert(error);

                this.setState({
                    error,
                    isLoading: false
                });
            });
        } catch (e) {
            errorAlert(e);
        }
    }

    render() {
        return <>
            <Row>
                <Form.Group>
                    <GoBackButton onClick={this.props.history.goBack} />
                </Form.Group>
                &nbsp;&nbsp;&nbsp;<h4>New Snapshot</h4>
            </Row>
            <br />
            <Row>
                <Col>
                    <Form.Group>
                        <DirectorySelector onDirectorySelected={p => this.setState({ path: p })} autoFocus placeholder="enter path to snapshot" name="path" value={this.state.path} onChange={this.handleChange} />
                    </Form.Group>
                </Col>
                <Col xs="auto">
                    <Button
                        size="sm"
                        disabled={!this.state.resolvedSource?.path}
                        title="Estimate"
                        variant="primary"
                        onClick={this.estimate}>Estimate</Button>
                    &nbsp;
                    <Button size="sm"
                        disabled={!this.state.resolvedSource?.path}
                        title="Snapshot Now"
                        variant="success"
                        onClick={this.snapshotNow}>Snapshot Now</Button>
                </Col>
            </Row>
            {this.state.estimateTaskID && this.state.estimateTaskVisible &&
                <EstimateResults taskID={this.state.estimateTaskID} hideDescription={true} showZeroCounters={true} />
            }
            <br />

            {this.state.resolvedSource && <Row><Col xs={12}>
                <Form.Text>
                    {this.state.resolvedSource ? this.state.resolvedSource.path : this.state.path}
                </Form.Text>

                <PolicyEditor ref={this.policyEditorRef}
                    embedded
                    host={this.state.resolvedSource.host}
                    userName={this.state.resolvedSource.userName}
                    path={this.state.resolvedSource.path} />
            </Col></Row>}

            <Row><Col>&nbsp;</Col></Row>

            <CLIEquivalent command={`snapshot create ${this.state.resolvedSource ? this.state.resolvedSource.path : this.state.path}`} />
        </>;
    }
}
