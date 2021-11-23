import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupB2 extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["bucket", "keyId", "key"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "B2 Bucket", "bucket", { autoFocus: true, placeholder: "enter bucket name" })}
            </Row>
            <Row>
                {RequiredField(this, "Key ID", "keyId", { placeholder: "enter application or account key ID" })}
                {RequiredField(this, "Key", "key", { placeholder: "enter secret application or account key", type: "password" })}
            </Row>
            <Row>
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Row>
        </>;
    }
}
