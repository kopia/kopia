import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupAzure extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["container", "storageAccount", "storageKey"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Container", "container", { placeholder: "enter container name" })}
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Access Key ID", "storageAccount", { placeholder: "enter access key ID" })}
                {RequiredField(this, "Secret Access Key", "storageKey", { placeholder: "enter secret access key", type: "password" })}
            </Form.Row>
        </>;
    }
}
