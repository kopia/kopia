import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupS3 extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["bucket", "endpoint", "accessKeyID", "secretAccessKey"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "S3 Bucket", "bucket", { placeholder: "enter bucket name" })}
                {RequiredField(this, "Server Endpoint", "endpoint", { placeholder: "enter server address (e.g. s3.amazonaws.com)" })}
                {OptionalField(this, "Override S3 Region", "region", { placeholder: "enter specific region (e.g. us-west-1) or leave empty" })}
            </Form.Row>
            <Form.Row>
                {RequiredField(this, "Access Key ID", "accessKeyID", { placeholder: "enter access key ID" })}
                {RequiredField(this, "Secret Access Key", "secretAccessKey", { placeholder: "enter secret access key", type: "password" })}
                {OptionalField(this, "Session Token", "sessionToken", { placeholder: "enter session token or leave empty", type: "password" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Form.Row>
        </>;
    }
}
