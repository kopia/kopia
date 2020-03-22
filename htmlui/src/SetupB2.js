import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupB2 extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["bucket", "keyId", "key"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "B2 Bucket", "bucket", { placeholder: "enter bucket name" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Key ID", "keyId", { placeholder: "enter application or account key ID" })}
                {RequiredField(this, "Key", "key", { placeholder: "enter secret application or account key", type: "password" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Form.Row>
        </>;
    }
}
