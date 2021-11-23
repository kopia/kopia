import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';


export class SetupGCS extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["bucket"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "GCS Bucket", "bucket", { autoFocus: true, placeholder: "enter bucket name" })}
                {OptionalField(this, "Object Name Prefix", "prefix", { placeholder: "enter object name prefix or leave empty", type: "password" })}
            </Row>
            <Row>
                {OptionalField(this, "Credentials File", "credentialsFile", { placeholder: "enter name of credentials JSON file" })}
            </Row>
            <Row>
                {OptionalField(this, "Credentials JSON", "credentials", { placeholder: "paste JSON credentials here", as: "textarea", rows: 5 })}
            </Row>
        </>;
    }
}
