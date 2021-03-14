import { faAngleDoubleDown, faAngleDoubleUp } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Collapse from 'react-bootstrap/Collapse';
import Form from 'react-bootstrap/Form';
import Spinner from 'react-bootstrap/Spinner';
import { handleChange, RequiredBoolean, RequiredField, validateRequiredFields } from './forms';
import { SetupAzure } from './SetupAzure';
import { SetupB2 } from "./SetupB2";
import { SetupFilesystem } from './SetupFilesystem';
import { SetupGCS } from './SetupGCS';
import { SetupKopiaServer } from './SetupKopiaServer';
import { SetupRclone } from './SetupRclone';
import { SetupS3 } from './SetupS3';
import { SetupSFTP } from './SetupSFTP';
import { SetupToken } from './SetupToken';
import { SetupWebDAV } from './SetupWebDAV';

const supportedProviders = [
    { provider: "filesystem", description: "Filesystem", component: SetupFilesystem },
    { provider: "gcs", description: "Google Cloud Storage", component: SetupGCS },
    { provider: "s3", description: "Amazon S3, Minio, Wasabi, etc.", component: SetupS3 },
    { provider: "b2", description: "Backblaze B2", component: SetupB2 },
    { provider: "azureBlob", description: "Azure Blob Storage", component: SetupAzure },
    { provider: "sftp", description: "SFTP server", component: SetupSFTP },
    { provider: "rclone", description: "Rclone remote", component: SetupRclone },
    { provider: "webdav", description: "WebDAV server", component: SetupWebDAV },
    { provider: "_server", description: "Kopia Repository Server", component: SetupKopiaServer },
    { provider: "_token", description: "Use Repository Token", component: SetupToken },
];

export class SetupRepository extends Component {
    constructor() {
        super();

        this.state = {
            confirmCreate: false,
            isLoading: false,
            showAdvanced: false,
            storageVerified: false,
            providerSettings: {},
            description: "My Repository",
        };

        this.handleChange = handleChange.bind(this);
        this.optionsEditor = React.createRef();
        this.connectToRepository = this.connectToRepository.bind(this);
        this.createRepository = this.createRepository.bind(this);
        this.cancelCreate = this.cancelCreate.bind(this);
        this.toggleAdvanced = this.toggleAdvanced.bind(this);
        this.verifyStorage = this.verifyStorage.bind(this);
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
        axios.get('/api/v1/current-user').then(result => {
            this.setState({
                username: result.data.username,
                hostname: result.data.hostname,
            });
        });
    }

    validate() {
        const ed = this.optionsEditor.current;

        let valid = true;

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

    createRepository(e) {
        e.preventDefault();

        if (!this.validate()) {
            return;
        }

        const request = {
            storage: {
                type: this.state.provider,
                config: this.state.providerSettings,
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
        };

        request.clientOptions = this.clientOptions();

        axios.post('/api/v1/repo/create', request).then(result => {
            window.location.replace("/");
        }).catch(error => {
            if (error.response.data) {
                this.setState({
                    connectError: error.response.data.code + ": " + error.response.data.error,
                });
            }
        });
    }

    connectToRepository(e) {
        e.preventDefault();
        if (!this.validate()) {
            return;
        }

        let request = null;
        switch (this.state.provider) {
            case "_token":
                request = {
                    token: this.state.providerSettings.token,
                };
                break;

            case "_server":
                request = {
                    apiServer: this.state.providerSettings,
                    password: this.state.password,
                };
                break;

            default:
                request = {
                    storage: {
                        type: this.state.provider,
                        config: this.state.providerSettings,
                    },
                    password: this.state.password,
                };
                break;
        }

        request.clientOptions = this.clientOptions();

        this.setState({ isLoading: true });
        axios.post('/api/v1/repo/connect', request).then(result => {
            this.setState({ isLoading: false });
            window.location.replace("/");
        }).catch(error => {
            this.setState({ isLoading: false });
            if (error.response.data) {
                this.setState({
                    confirmCreate: false,
                    connectError: error.response.data.code + ": " + error.response.data.error,
                });
            }
        });
    }

    clientOptions() {
        return {
            description: this.state.description,
            username: this.state.username,
            readonly: this.state.readonly,
            hostname: this.state.hostname,
        };
    }

    toggleAdvanced() {
        this.setState({ showAdvanced: !this.state.showAdvanced });
    }

    cancelCreate() {
        this.setState({ confirmCreate: false });
    }

    renderProviderSelection() {
        return <>
            <h3>Select Storage Type</h3>
            <p>To connect to a repository or create one, select the preferred storage type.</p>
            <Form.Row>
                {supportedProviders.map(x =>
                    <Button key={x.provider}
                        data-testid={'provider-' + x.provider}
                        onClick={() => this.setState({ provider: x.provider, providerSettings: {} })}
                        variant={x.provider.startsWith("_") ? "outline-success" : "outline-primary"}
                        className="providerIcon" >{x.description}</Button>
                )}
            </Form.Row>
        </>;
    }

    verifyStorage(e) {
        e.preventDefault();

        const ed = this.optionsEditor.current;
        if (ed && !ed.validate()) {
            return;
        }

        if (this.state.provider === "_token" || this.state.provider === "_server") {
            this.setState({
                // for token and server assume it's verified and exists, if not, will fail in the next step.
                storageVerified: true,
                confirmCreate: false,
                isLoading: false,
                providerSettings: ed.state,
            });
            return;
        }

        const request = {
            storage: {
                type: this.state.provider,
                config: ed.state,
            },
        };

        this.setState({ isLoading: true });
        axios.post('/api/v1/repo/exists', request).then(result => {
            this.setState({
                // verified and exists
                storageVerified: true,
                confirmCreate: false,
                isLoading: false,
                providerSettings: ed.state,
            });
        }).catch(error => {
            this.setState({ isLoading: false });
            if (error.response.data) {
                if (error.response.data.code === "NOT_INITIALIZED") {
                    this.setState({
                        // verified and does not exist
                        confirmCreate: true,
                        storageVerified: true,
                        providerSettings: ed.state,
                        connectError: null,
                    });
                } else {
                    this.setState({
                        connectError: error.response.data.code + ": " + error.response.data.error,
                    });
                }
            } else {
                this.setState({
                    connectError: error.message,
                });
            }
        });
    }

    renderProviderConfiguration() {
        let SelectedProvider = null;
        for (const prov of supportedProviders) {
            if (prov.provider === this.state.provider) {
                SelectedProvider = prov.component;
            }
        }

        return <Form onSubmit={this.verifyStorage}>
            {!this.state.provider.startsWith("_") && <h3>Storage Configuration</h3>}
            {this.state.provider === "_token" && <h3>Enter Repository Token</h3>}
            {this.state.provider === "_server" && <h3>Kopia Server Parameters</h3>}

            <SelectedProvider ref={this.optionsEditor} initial={this.state.providerSettings} />

            {this.connectionErrorInfo()}
            <hr />

            <Button variant="secondary" onClick={() => this.setState({ provider: null, providerSettings: null, connectError: null })}>Back</Button>
            &nbsp;
            <Button variant="primary" type="submit" data-testid="submit-button">Next</Button>
            {this.loadingSpinner()}
        </Form>;
    }

    toggleAdvancedButton() {
        return <Button onClick={this.toggleAdvanced}
            variant="secondary"
            aria-controls="advanced-options-div"
            aria-expanded={this.state.showAdvanced}
            size="sm"
        >
            {!this.state.showAdvanced ? <>
                <FontAwesomeIcon icon={faAngleDoubleDown} />&nbsp;Show Advanced Options
                            </> : <>
                <FontAwesomeIcon icon={faAngleDoubleUp} />&nbsp;Hide Advanced Options
                            </>}
        </Button>;
    }

    renderConfirmCreate() {
        return <Form onSubmit={this.createRepository}>
            <h3>Create New Repository</h3>
            <p>Enter a strong password to create Kopia repository in the provided storage.</p>
            <Form.Row>
                {RequiredField(this, "Repository Password", "password", { autoFocus: true, type: "password", placeholder: "enter repository password" }, "The password used to encrypt repository contents.")}
                {RequiredField(this, "Confirm Repository Password", "confirmPassword", { type: "password", placeholder: "enter repository password again" })}
            </Form.Row>
            {this.toggleAdvancedButton()}
            <Collapse in={this.state.showAdvanced}>
                <div id="advanced-options-div">
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
                    {this.overrideUsernameHostnameRow()}
                    <Form.Row>
                        <Form.Group as={Col}>
                            <Form.Text>Additional parameters can be set when creating repository using command line</Form.Text>
                        </Form.Group>
                    </Form.Row>
                </div>
            </Collapse>
            {this.connectionErrorInfo()}
            <hr />
            <Button variant="secondary" onClick={() => this.setState({ storageVerified: false })}>Back</Button>
            &nbsp;
            <Button variant="success" type="submit" data-testid="submit-button">Create Repository</Button>
            {this.loadingSpinner()}
        </Form>;
    }

    overrideUsernameHostnameRow() {
        return <Form.Row>
            {RequiredField(this, "Username", "username", {}, "Override this when restoring snapshot taken by another user.")}
            {RequiredField(this, "Hostname", "hostname", {}, "Override this when restoring snapshot taken on another machine.")}
        </Form.Row>;
    }

    connectionErrorInfo() {
        return this.state.connectError && <Form.Row>
            <Form.Group as={Col}>
                <Form.Text className="error">Connect Error: {this.state.connectError}</Form.Text>
            </Form.Group>
        </Form.Row>;
    }

    renderConfirmConnect() {
        return <Form onSubmit={this.connectToRepository}>
            <h3>Connect To Repository</h3>
            <Form.Row>
                <Form.Group as={Col}>
                    <Form.Label className="required">Connect As</Form.Label>
                    <Form.Control
                        value={this.state.username + '@' + this.state.hostname}
                        readOnly={true}
                        size="sm" />
                    <Form.Text className="text-muted">To override, click 'Show Advanced Options'</Form.Text>
                </Form.Group>
            </Form.Row>
            <Form.Row>
                {(this.state.provider !== "_token" && this.state.provider !== "_server") && RequiredField(this, "Repository Password", "password", { autoFocus: true, type: "password", placeholder: "enter repository password" }, "The password used to encrypt repository contents.")}
                {this.state.provider === "_server" && RequiredField(this, "Server Password", "password", { autoFocus: true, type: "password", placeholder: "enter password to connect to server" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Repository Description", "description", { autoFocus: this.state.provider === "_token", placeholder: "enter repository description" }, "Description helps you distinguish between multiple connected repositories.")}
            </Form.Row>
            {this.toggleAdvancedButton()}
            <Collapse in={this.state.showAdvanced}>
                <div id="advanced-options-div" className="advancedOptions">
                    <Form.Row>
                        {RequiredBoolean(this, "Connect in read-only mode", "readonly", "Read-only mode prevents any changes to the repository.")}
                    </Form.Row>
                    {this.overrideUsernameHostnameRow()}
                </div>
            </Collapse>
            {this.connectionErrorInfo()}
            <hr />
            <Button variant="secondary" onClick={() => this.setState({ storageVerified: false })}>Back</Button>
            &nbsp;
            <Button variant="success" type="submit" data-testid="submit-button">Connect To Repository</Button>
            {this.loadingSpinner()}
        </Form>;
    }

    renderInternal() {
        if (!this.state.provider) {
            return this.renderProviderSelection()
        }

        if (!this.state.storageVerified) {
            return this.renderProviderConfiguration();
        }

        if (this.state.confirmCreate) {
            return this.renderConfirmCreate();
        }

        return this.renderConfirmConnect();
    }

    loadingSpinner() {
        return this.state.isLoading && <Spinner animation="border" variant="primary" />;
    }

    render() {
        return <>
            {this.renderInternal()}
            {/* <pre className="debug-json">{JSON.stringify(this.state, null, 2)}</pre> */}
        </>;
    }
}
