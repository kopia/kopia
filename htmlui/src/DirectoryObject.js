import { faCopy } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Button from 'react-bootstrap/Button';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Spinner from 'react-bootstrap/Spinner';
import { DirectoryItems } from "./DirectoryItems";
import { CLIEquivalent, GoBackButton } from './uiutil';


export class DirectoryObject extends Component {
    constructor() {
        super();

        this.state = {
            items: [],
            isLoading: false,
            error: null,
            mountInfo: {},
            oid: "",
        };

        this.mount = this.mount.bind(this);
        this.unmount = this.unmount.bind(this);
        this.browseMounted = this.browseMounted.bind(this);
        this.copyPath = this.copyPath.bind(this);
    }

    componentDidMount() {
        this.fetchDirectory(this.props);
    }

    fetchDirectory(props) {
        let oid = props.match.params.oid;

        this.setState({
            isLoading: true,
            oid: oid,
        });

        axios.get('/api/v1/objects/' + oid).then(result => {
            this.setState({
                items: result.data.entries || [],
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));

        axios.get('/api/v1/mounts/' + oid).then(result => {
            this.setState({
                mountInfo: result.data,
            });
        }).catch(error => this.setState({
            mountInfo: {},
        }));
    }

    componentWillReceiveProps(props) {
        this.fetchDirectory(props);
    }

    mount() {
        axios.post('/api/v1/mounts', { "root": this.state.oid }).then(result => {
            this.setState({
                mountInfo: result.data,
            });
        }).catch(error => this.setState({
            mountInfo: {},
        }));
    }

    unmount() {
        axios.delete('/api/v1/mounts/' + this.state.oid).then(result => {
            this.setState({
                mountInfo: {},
            });
        }).catch(error => this.setState({
            error: error,
            mountInfo: {},
        }));
    }

    browseMounted() {
        if (!window.kopiaUI) {
            alert('Directory browsing is not supported in a web browser. Use Kopia UI.');
            return;
        }

        window.kopiaUI.browseDirectory(this.state.mountInfo.path);
    }

    copyPath() {
        const el = document.querySelector("#mountedPath");
        if (!el) {
            return
        }

        el.select();
        el.setSelectionRange(0, 99999);

        document.execCommand("copy");
    }

    render() {
        let { items, isLoading, error } = this.state;
        if (error) {
            return <p>ERROR: {error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        return <div className="padded">
            <Row>
                <Col xs={12}>
                    <GoBackButton onClick={this.props.history.goBack} />
                    &nbsp;
                    {this.state.mountInfo.path ? <>
                        <Button size="sm" variant="info" onClick={this.unmount} >Unmount</Button>
                        {window.kopiaUI && <>
                            &nbsp;
                            <Button size="sm" variant="info" onClick={this.browseMounted} >Browse</Button>
                        </>}
                        &nbsp;<input id="mountedPath" value={this.state.mountInfo.path} />
                        <Button size="sm" variant="primary" onClick={this.copyPath} ><FontAwesomeIcon icon={faCopy} /></Button>
                    </> : <>
                        <Button size="sm" variant="primary" onClick={this.mount} >Mount</Button>
                    </>}
                    &nbsp;
                    <Button size="sm" variant="info" href={"/snapshots/dir/" + this.props.match.params.oid + "/restore"}>Restore...</Button>
                </Col>
            </Row>
            <Row><Col>&nbsp;</Col>
            </Row>
            <Row>
                <Col xs={12}><DirectoryItems items={items} /></Col>
            </Row>
            <CLIEquivalent command={`snapshot list ${this.state.oid}`} />
        </div>
    }
}
