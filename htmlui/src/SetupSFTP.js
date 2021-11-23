import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, hasExactlyOneOf, OptionalField, OptionalNumberField, RequiredBoolean, RequiredField, validateRequiredFields } from './forms';

export class SetupSFTP extends Component {
    constructor(props) {
        super();

        this.state = {
            port: 22,
            validated: false,
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        this.setState({
            validated: true,
        });

        if (!validateRequiredFields(this, ["host", "port", "username", "path"])) {
            return false;
        }

        if (this.state.externalSSH) {
            return true
        }

        if (!hasExactlyOneOf(this, ["password", "keyfile", "keyData"])) {
            return false;
        }

        if (!hasExactlyOneOf(this, ["knownHostsFile", "knownHostsData"])) {
            return false;
        }

        return true;
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "Host", "host", { autoFocus: true, placeholder: "ssh host name (e.g. example.com)" })}
                {RequiredField(this, "User", "username", { placeholder: "user name" })}
                {OptionalNumberField(this, "Port", "port", { placeholder: "port number (e.g. 22)" })}
            </Row>
            <Row>
                {RequiredField(this, "Path", "path", { placeholder: "enter remote path to repository, e.g. '/mnt/data/repository'" })}
            </Row>
            {!this.state.externalSSH && <>
                <Row>
                    {OptionalField(this, "Password", "password", { type: "password", placeholder: "password" })}
                </Row>
                <Row>
                    {OptionalField(this, "Path to key file", "keyfile", { placeholder: "enter path to the key file" })}
                    {OptionalField(this, "Path to known_hosts File", "knownHostsFile", { placeholder: "enter path to known_hosts file" })}
                </Row>
                <Row>
                    {OptionalField(this, "Key Data", "keyData", {
                        placeholder: "paste contents of the key file",
                        as: "textarea",
                        rows: 5,
                        isInvalid: this.state.validated && !this.state.externalSSH && !hasExactlyOneOf(this, ["password", "keyfile", "keyData"]),
                    }, null, <>One of <b>Password</b>, <b>Key File</b> or <b>Key Data</b> is required.</>)}
                    {OptionalField(this, "Known Hosts Data", "knownHostsData", {
                        placeholder: "paste contents of the known_hosts file",
                        as: "textarea",
                        rows: 5,
                        isInvalid: this.state.validated && !this.state.externalSSH && !hasExactlyOneOf(this, ["knownHostsFile", "knownHostsData"]),
                    }, null, <>Either <b>Known Hosts File</b> or <b>Known Hosts Data</b> is required, but not both.</>)}
                </Row>
                <hr/>
            </>}
            {RequiredBoolean(this, "Launch external password-less SSH command", "externalSSH", "By default Kopia connects to the server using internal SSH client which supports limited options. Alternatively it may launch external password-less SSH command, which supports additional options, but is generally less efficient than the built-in client.")}
            {this.state.externalSSH && <><Row>
                {OptionalField(this, "SSH Command", "sshCommand", { placeholder: "provide enter passwordless SSH command to execute (typically 'ssh')" })}
                {OptionalField(this, "SSH Arguments", "sshArguments", { placeholder: "enter SSH command arguments ('user@host -s sftp' will be appended automatically)" })}
            </Row></>}

        </>;
    }
}
