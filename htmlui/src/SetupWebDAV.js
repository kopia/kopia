import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

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
            <Row>
            {RequiredField(this, "WebDAV Server URL", "url", { autoFocus: true, placeholder: "http[s]://server:port/path" })}
            </Row>
            <Row>
            {OptionalField(this, "Username", "username", { placeholder: "enter username" })}
            {OptionalField(this, "Password", "password", { placeholder: "enter password", type: "password" })}
            </Row>
        </>;
    }
}
