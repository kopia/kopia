import { faWindowClose } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import { handleChange, validateRequiredFields } from './forms';
import { PolicyEditor } from './PolicyEditor';
import { TaskDetails } from './TaskDetails';
import { cancelTask, DirectorySelector, errorAlert, GoBackButton, redirectIfNotConnected } from './uiutil';

export class NewSnapshot extends Component {
    constructor() {
        super();
        this.state = {
            path: "",
            estimateTaskID: null,
            estimateTaskVisible: false,
            policyEditorVisibleFor: "n/a",
            localUsername: null,
        };

        this.policyEditorRef = React.createRef();
        this.handleChange = handleChange.bind(this);
        this.estimate = this.estimate.bind(this);
        this.snapshotNow = this.snapshotNow.bind(this);
        this.cancelEstimate = this.cancelEstimate.bind(this);
        this.togglePolicyEditor = this.togglePolicyEditor.bind(this);
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

    togglePolicyEditor() {
        if (!this.state.path) {
            return;
        }

        if (this.state.policyEditorVisibleFor === this.state.path) {
            this.setState({
                policyEditorVisibleFor: "n/a",
            });
        } else {
            this.setState({
                policyEditorVisibleFor: this.state.path,
            });
        }
    }

    estimate(e) {
        e.preventDefault();

        if (!validateRequiredFields(this, ["path"])) {
            return;
        }

        let req = {
            root: this.state.path,
        }

        axios.post('/api/v1/estimate', req).then(result => {
            this.setState({
                estimateTaskID: result.data.id,
                estimatingPath: result.data.description,
                estimateTaskVisible: true,
                didEstimate: false,
            })
        }).catch(error => {
            errorAlert(error);
        });
    }

    cancelEstimate() {
        this.setState({ estimateTaskVisible: false });
        cancelTask(this.state.estimateTaskID);
    }

    snapshotNow(e) {
        e.preventDefault();

        if (!this.state.path) {
            alert('Must specify directory to snapshot.');
            return
        }

        axios.post('/api/v1/sources', {
            path: this.state.path,
            createSnapshot: true,
        }).then(result => {
            this.props.history.goBack();
        }).catch(error => {
            errorAlert(error);

            this.setState({
                error,
                isLoading: false
            });
        });
    }

    render() {
        return <>
            <Form.Row>
                <Form.Group>
                    <GoBackButton onClick={this.props.history.goBack} />
                </Form.Group>
                &nbsp;&nbsp;&nbsp;<h4>New Snapshot</h4>
            </Form.Row>
            <br />
            <Form.Row>
                <Col>
                    <Form.Group>
                        <DirectorySelector onDirectorySelected={p => this.setState({ path: p })} autoFocus placeholder="enter path to snapshot" name="path" value={this.state.path} onChange={this.handleChange}
                            readOnly={this.state.policyEditorVisible || this.state.estimateTaskVisible} />
                        <Form.Text>
                            Click the <code>Estimate</code> button to estimate the size of the snapshot.
                            To specify frequency of snapshots or exclude some files, click <code>Policy</code>.
                        </Form.Text>
                    </Form.Group>
                </Col>
                <Col xs="auto">
                    <Button
                        size="sm"
                        disabled={!this.state.path}
                        title="Edit Policy"
                        variant="primary"
                        onClick={this.estimate}>Estimate</Button>
                </Col>
                <Col xs="auto">
                    <Button
                        size="sm"
                        disabled={!this.state.path}
                        title="Estimate"
                        variant="primary"
                        onClick={this.togglePolicyEditor}>Policy</Button>
                </Col>
            </Form.Row>
            {this.state.estimateTaskID && this.state.estimateTaskVisible &&
                <div className="estimate-results">
                    <h5>
                        <Button onClick={this.cancelEstimate} title="stop estimation" variant="light" size="sm"><FontAwesomeIcon color="#888" icon={faWindowClose} /></Button>&nbsp;
                        Estimate Snapshot Size for {this.state.estimatingPath}...
                    </h5>
                    <TaskDetails taskID={this.state.estimateTaskID} hideDescription={true} showZeroCounters={true} />
                </div>
            }
            <br />

            {this.state.path && this.state.policyEditorVisibleFor === this.state.path && <Form.Row>
                <Col xs={12}>
                    <PolicyEditor ref={this.policyEditorRef} 
                    embedded 
                    host={this.state.localHost}
                    userName={this.state.localUsername}
                    path={this.state.path} 
                    close={this.togglePolicyEditor} />
                </Col>
            </Form.Row>}

            <Form.Row>
                <Button size="sm"
                    disabled={!this.state.path}
                    title="Snapshot Now"
                    variant="primary"
                    onClick={this.snapshotNow}
                >Snapshot Now</Button>
            </Form.Row>
        </>;
    }
}
