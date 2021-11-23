import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, RequiredField, validateRequiredFields } from './forms';

export class SetupToken extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["token"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "Token", "token", { autoFocus: true, type: "password", placeholder: "paste connection token" })}
            </Row>
        </>;
    }
}
