import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, OptionalNumberField, RequiredField, validateRequiredFields, hasExactlyOneOf } from './forms';

export class SetupSFTP extends Component {
    constructor() {
        super();

        this.state = {
            port: 22,
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        if (!validateRequiredFields(this, ["host", "port", "username", "path"])) {
            return false;
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
                {RequiredField(this, "Host", "host", { placeholder: "host name" })}
                {OptionalNumberField(this, "Port", "port", { placeholder: "port number (e.g. 22)" })}
                {RequiredField(this, "User", "username", { placeholder: "user name" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Path", "path", { placeholder: "enter remote path" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Path to key file", "keyfile", { placeholder: "enter path to the key file" })}
                {OptionalField(this, "Path to known_hosts File", "knownHostsFile", { placeholder: "enter path to known_hosts file" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Key Data", "keyData", { 
                    placeholder: "paste contents of the key file", 
                    as: "textarea", 
                    rows: 5,
                    isInvalid: !hasExactlyOneOf(this, ["keyfile", "keyData"]),
                }, null, <>Either <b>Key File</b> or <b>Key Data</b> is required, but not both.</>)}
                {OptionalField(this, "Known Hosts Data", "knownHostsData", { 
                    placeholder: "paste contents of the known_hosts file", 
                    as: "textarea", 
                    rows: 5,
                    isInvalid: !hasExactlyOneOf(this, ["knownHostsFile", "knownHostsData"]),
                }, null, <>Either <b>Known Hosts File</b> or <b>Known Hosts Data</b> is required, but not both.</>)}
            </Form.Row>
        </>;
    }
}
