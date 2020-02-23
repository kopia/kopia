import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Spinner from 'react-bootstrap/Spinner';
import { SetupRepository } from './SetupRepository';

export class RepoStatus extends Component {
    constructor() {
        super();

        this.state = {
            status: {},
            isLoading: true,
            error: null,
            provider: "",
        };

        this.mounted = false;
        this.disconnect = this.disconnect.bind(this);
    }

    componentDidMount() {
        this.mounted = true;
        this.fetchStatus(this.props);
    }

    componentWillUnmount() {
        this.mounted = false;
    }

    fetchStatus(props) {
        if (this.mounted) {
            this.setState({
                isLoading: true,
            });
        }

        axios.get('/api/v1/repo/status').then(result => {
            if (this.mounted) {
                this.setState({
                    status: result.data,
                    isLoading: false,
                });
            }
        }).catch(error => {
            if (this.mounted) {
                this.setState({
                    error,
                    isLoading: false
                })
            }
        });
    }

    disconnect() {
        this.setState({ isLoading: true })
        axios.post('/api/v1/repo/disconnect', {}).then(result => {
            window.location.replace("/");
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    selectProvider(provider) {
        this.setState({ provider });
    }

    render() {
        let { isLoading, error } = this.state;
        if (error) {
            return <p>ERROR: {error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        return this.state.status.connected ?
            <>
                <h3>Connected To Repository</h3>
                <Form>
                    <Form.Group>
                        <Form.Label>Config File</Form.Label>
                        <Form.Control readOnly defaultValue={this.state.status.configFile} />
                    </Form.Group>
                    <Form.Group>
                        <Form.Label>Cache Directory</Form.Label>
                        <Form.Control readOnly defaultValue={this.state.status.cacheDir} />
                    </Form.Group>
                    <Form.Row>
                        <Form.Group as={Col}>
                            <Form.Label>Provider</Form.Label>
                            <Form.Control readOnly defaultValue={this.state.status.storage} />
                        </Form.Group>
                        <Form.Group as={Col}>
                            <Form.Label>Hash Algorithm</Form.Label>
                            <Form.Control readOnly defaultValue={this.state.status.hash} />
                        </Form.Group>
                        <Form.Group as={Col}>
                            <Form.Label>Encryption Algorithm</Form.Label>
                            <Form.Control readOnly defaultValue={this.state.status.encryption} />
                        </Form.Group>
                        <Form.Group as={Col}>
                            <Form.Label>Splitter Algorithm</Form.Label>
                            <Form.Control readOnly defaultValue={this.state.status.splitter} />
                        </Form.Group>
                    </Form.Row>
                    <Button variant="danger" onClick={this.disconnect}>Disconnect</Button>
                </Form>
                <hr />
            </> : <>
                <h3>Setup Repository</h3>
                <p>Before you can use Kopia, you must connect to a repository.
                    Select a provider blow to connect to storage where you want to store Kopia backups.</p>

                <SetupRepository />
            </>;
    }
}
