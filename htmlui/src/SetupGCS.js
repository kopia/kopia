import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';

import { validateRequiredFields, handleChange, RequiredField, OptionalField } from './forms';

export class SetupGCS extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["bucket"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "GCS Bucket", "bucket", { placeholder: "enter bucket name" })}
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Credentials File", "credentialsFile", { placeholder: "enter name of credentials JSON file" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Credentials JSON", "credentials", { placeholder: "paste JSON credentials here", as: "textarea", rows: 5 })}
            </Form.Row>
        </>;
    }
}
