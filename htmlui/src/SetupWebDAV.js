import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { validateRequiredFields, handleChange, RequiredField, OptionalField } from './forms';

export class SetupWebDAV extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["url"])
    }

    render() {
        return <>
            <Form.Row>
            {RequiredField(this, "WebDAV Server URL", "url", { autoFocus: true, placeholder: "http[s]://server:port/path" })}
            </Form.Row>
            <Form.Row>
            {OptionalField(this, "Username", "username", { placeholder: "enter username" })}
            {OptionalField(this, "Password", "password", { placeholder: "enter password", type: "password" })}
            </Form.Row>
        </>;
    }
}
