import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Spinner from 'react-bootstrap/Spinner';
import { handleChange, RequiredField, validateRequiredFields } from './forms';
import { SetupFilesystem } from './SetupFilesystem';
import { SetupGCS } from './SetupGCS';
import { SetupS3 } from './SetupS3';
import { SetupAzure } from './SetupAzure';
import { SetupSFTP } from './SetupSFTP';
import { SetupToken } from './SetupToken';
import { SetupWebDAV } from './SetupWebDAV';

const supportedProviders = [
    { provider: "filesystem", description: "Filesystem", component: SetupFilesystem },
    { provider: "gcs", description: "Google Cloud Storage", component: SetupGCS },
    { provider: "s3", description: "Amazon S3, Minio, Wasabi, etc.", component: SetupS3 },
    { provider: "azureBlob", description: "Azure Blob Storage", component: SetupAzure },
    { provider: "sftp", description: "SFTP server", component: SetupSFTP },
    { provider: "webdav", description: "WebDAV server", component: SetupWebDAV },
    { provider: "_token", description: "(use token)", component: SetupToken },
]

export class SetupRepository extends Component {
    constructor() {
        super();

        this.state = {
            confirmCreate: false,
            isLoading: false,
        };

        this.handleChange = handleChange.bind(this);
        this.optionsEditor = React.createRef();
        this.initRepository = this.initRepository.bind(this);
        this.connectToRepository = this.connectToRepository.bind(this);
        this.cancelCreate = this.cancelCreate.bind(this);
    }

    componentDidMount() {
        axios.get('/api/v1/repo/algorithms').then(result => {
            this.setState({
                algorithms: result.data,
                hash: result.data.defaultHash,
                encryption: result.data.defaultEncryption,
                splitter: result.data.defaultSplitter,
            });
        });
    }

    validate() {
        const ed = this.optionsEditor.current;

        let valid = true

        if (this.state.provider !== "_token") {
            if (!validateRequiredFields(this, ["password"])) {
                valid = false;
            }
        }

        if (ed && !ed.validate()) {
            valid = false;
        }

        if (this.state.confirmCreate) {
            if (!validateRequiredFields(this, ["confirmPassword"])) {
                valid = false;
            }

            if (valid && this.state.password !== this.state.confirmPassword) {
                alert("Passwords don't match");
                return false;
            }
        }

        return valid;
    }

    initRepository() {
        if (!this.validate()) {
            return;
        }

        const ed = this.optionsEditor.current;
        if (!ed) {
            return
        }

        const request = {
            storage: {
                type: this.state.provider,
                config: ed.state,
            },
            password: this.state.password,
            options: {
                blockFormat: {
                    hash: this.state.hash,
                    encryption: this.state.encryption,
                },
                objectFormat: {
                    splitter: this.state.splitter,
                },
            },
        }

        axios.post('/api/v1/repo/create', request).then(result => {
            window.location.replace("/");
        }).catch(error => {
            alert('failed to create repository: ' + JSON.stringify(error.response.data));
        });
    }

    connectToRepository() {
        if (!this.validate()) {
            return;
        }

        const ed = this.optionsEditor.current;
        if (!ed) {
            return
        }

        let request = null;
        if (this.state.provider === "_token") {
            request = {
                token: ed.state.token,
            }
        } else {
            request = {
                storage: {
                    type: this.state.provider,
                    config: ed.state,
                },
                password: this.state.password,
            }
        }

        this.setState({ isLoading: true });
        axios.post('/api/v1/repo/connect', request).then(result => {
            this.setState({ isLoading: false });
            window.location.replace("/");
        }).catch(error => {
            this.setState({ isLoading: false });
            if (error.response.data) {
                if (error.response.data.code === "NOT_INITIALIZED") {
                    this.setState({
                        confirmCreate: true,
                        connectError: null,
                    });
                } else {
                    this.setState({
                        confirmCreate: false,
                        connectError: error.response.data.code + ": " + error.response.data.error,
                    });
                }
            }
        });
    }

    cancelCreate() {
        this.setState({ confirmCreate: false });
    }

    render() {
        let SelectedProvider = null;
        for (const prov of supportedProviders) {
            if (prov.provider === this.state.provider) {
                SelectedProvider = prov.component;
            }
        }

        return <Form className="providerParams">
            {!this.state.confirmCreate && <Form.Row>
                <Form.Group as={Col}>
                    <Form.Label className="required">Provider</Form.Label>
                    <Form.Control
                        name="provider"
                        value={this.state.provider}
                        onChange={this.handleChange}
                        data-testid="providerSelector"
                        as="select">
                        <option value="">(select)</option>
                        {supportedProviders.map(x => <option key={x.provider} value={x.provider}>{x.description}</option>)}
                    </Form.Control>
                </Form.Group>
            </Form.Row>}
            {SelectedProvider && <>
                <div className={this.state.confirmCreate ? 'hidden' : 'normal'}>
                    <SelectedProvider ref={this.optionsEditor} />
                </div>
                {this.state.confirmCreate && <>
                    <Form.Row>
                        <Form.Group as={Col}>
                            <Form.Label>Kopia repository was not found in the provided location and needs to be set up.<br />Please provide strong password to protect repository contents and optionally choose additional parameters.</Form.Label>
                        </Form.Group>
                    </Form.Row>
                </>}
                {this.state.provider !== "_token" && <Form.Row>
                    {RequiredField(this, "Repository Password", "password", { type: "password", placeholder: "enter repository password" })}
                    {this.state.confirmCreate && RequiredField(this, "Confirm Repository Password", "confirmPassword", { type: "password", placeholder: "enter repository password again" })}
                </Form.Row>}
                {!this.state.confirmCreate && <Button variant="primary" data-testid="connect-to-repository" onClick={this.connectToRepository} disabled={this.state.isLoading}>{this.state.isLoading && <Spinner animation="border" variant="light" size="sm" />} Connect To Repository</Button>}
                {this.state.connectError && <Form.Row>
                    <Form.Group as={Col}>
                        <Form.Text className="error">Connect Error: {this.state.connectError}</Form.Text>
                    </Form.Group>
                </Form.Row>}
                {this.state.confirmCreate && <>
                    <Form.Row>
                        <Form.Group as={Col}>
                            <Form.Label className="required">Encryption</Form.Label>
                            <Form.Control as="select"
                                name="encryption"
                                onChange={this.handleChange}
                                data-testid="control-encryption"
                                value={this.state.encryption}>
                                {this.state.algorithms.encryption.map(x => <option key={x} value={x}>{x}</option>)}
                            </Form.Control>
                        </Form.Group>
                        <Form.Group as={Col}>
                            <Form.Label className="required">Hash Algorithm</Form.Label>
                            <Form.Control as="select"
                                name="hash"
                                onChange={this.handleChange}
                                data-testid="control-hash"
                                value={this.state.hash}>
                                {this.state.algorithms.hash.map(x => <option key={x} value={x}>{x}</option>)}
                            </Form.Control>
                        </Form.Group>
                        <Form.Group as={Col}>
                            <Form.Label className="required">Splitter</Form.Label>
                            <Form.Control as="select"
                                name="splitter"
                                onChange={this.handleChange}
                                data-testid="control-splitter"
                                value={this.state.splitter}>
                                {this.state.algorithms.splitter.map(x => <option key={x} value={x}>{x}</option>)}
                            </Form.Control>
                        </Form.Group>
                    </Form.Row>
                    <Form.Row>
                        <Form.Group as={Col}>
                            <Form.Text>Additional parameters can be set when creating repository using command line</Form.Text>
                        </Form.Group>
                    </Form.Row>
                    <Button data-testid="create-repository" variant="primary" onClick={this.initRepository} disabled={this.state.isLoading}>{this.state.isLoading && <Spinner animation="border" variant="light" size="sm" />} Initialize Repository</Button>&nbsp;
                    <Button variant="outline-secondary" onClick={this.cancelCreate} >Cancel</Button>
                </>}
            </>
            }
            {/* <pre className="debug-json">
                {JSON.stringify(this.state)}
            </pre> */}
        </Form>;
    }
}
