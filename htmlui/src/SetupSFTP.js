import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, RequiredField, validateRequiredFields } from './forms';

export class SetupSFTP extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["host", "port", "path", "keyFile"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Host", "host", { placeholder: "host name" })}
                {RequiredField(this, "Port", "port", { placeholder: "port number (e.g. 22)", defaultValue: "22" })}
                {RequiredField(this, "Path", "path", { placeholder: "enter remote path" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Key File", "key file", { placeholder: "enter path to the key file" })}
            </Form.Row>
        </>;
    }
}
