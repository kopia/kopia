import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, OptionalNumberField, RequiredField, validateRequiredFields, hasExactlyOneOf, RequiredBoolean } from './forms';

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

        if (!hasExactlyOneOf(this, ["keyfile", "keyData"])) {
            return false;
        }

        if (!hasExactlyOneOf(this, ["knownHostsFile", "knownHostsData"])) {
            return false;
        }

        return true;
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Host", "host", { autoFocus: true, placeholder: "ssh host name (e.g. example.com)" })}
                {RequiredField(this, "User", "username", { placeholder: "user name" })}
                {OptionalNumberField(this, "Port", "port", { placeholder: "port number (e.g. 22)" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Path", "path", { placeholder: "enter remote path to repository, e.g. '/mnt/data/repository'" })}
            </Form.Row>
            {!this.state.externalSSH && <>
            <Form.Row>
                {OptionalField(this, "Path to key file", "keyfile", { placeholder: "enter path to the key file" })}
                {OptionalField(this, "Path to known_hosts File", "knownHostsFile", { placeholder: "enter path to known_hosts file" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Key Data", "keyData", { 
                    placeholder: "paste contents of the key file", 
                    as: "textarea", 
                    rows: 5,
                    isInvalid: this.state.validated && !this.state.externalSSH && !hasExactlyOneOf(this, ["keyfile", "keyData"]),
                }, null, <>Either <b>Key File</b> or <b>Key Data</b> is required, but not both.</>)}
                {OptionalField(this, "Known Hosts Data", "knownHostsData", { 
                    placeholder: "paste contents of the known_hosts file", 
                    as: "textarea", 
                    rows: 5,
                    isInvalid: this.state.validated && !this.state.externalSSH && !hasExactlyOneOf(this, ["knownHostsFile", "knownHostsData"]),
                }, null, <>Either <b>Known Hosts File</b> or <b>Known Hosts Data</b> is required, but not both.</>)}
            </Form.Row>
            </>}
            {RequiredBoolean(this, "Launch external password-less SSH command", "externalSSH", "By default Kopia connects to the server using internal SSH client which supports limited options. Alternatively it may launch external password-less SSH command, which supports additional options.")}
            {this.state.externalSSH && <><Form.Row>
                {OptionalField(this, "SSH Command", "sshCommand", { placeholder: "provide enter passwordless SSH command to execute (typically 'ssh')" })}
                {OptionalField(this, "SSH Arguments", "sshArguments", { placeholder: "enter SSH command arguments ('user@host -s sftp' will be appended automatically)" })}
            </Form.Row></>}

        </>;
    }
}
