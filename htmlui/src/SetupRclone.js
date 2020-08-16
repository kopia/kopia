import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupRclone extends Component {
    constructor() {
        super();

        this.state = {};
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["remotePath"])
    }

    render() {
        return <>
            <Form.Row>
                {RequiredField(this, "Rclone Remote Path", "remotePath", { placeholder: "enter <name-of-rclone-remote>:<path>" })}
            </Form.Row>
            <Form.Row>
                {OptionalField(this, "rclone executable", "rcloneExe", { placeholder: "enter path to rclone executable" })}
            </Form.Row>
        </>;
    }
}
