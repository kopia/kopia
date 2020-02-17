import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, RequiredField, validateRequiredFields, OptionalNumberField } from './forms';

export class SetupSFTP extends Component {
    constructor() {
        super();

        this.state = {
            port: 22,
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["host", "port", "path", "keyfile"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Host", "host", { placeholder: "host name" })}
                {OptionalNumberField(this, "Port", "port", { placeholder: "port number (e.g. 22)" })}
                {RequiredField(this, "Path", "path", { placeholder: "enter remote path" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Key File", "keyfile", { placeholder: "enter path to the key file" })}
            </Form.Row>
        </>;
    }
}
