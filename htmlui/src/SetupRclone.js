import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, OptionalField, RequiredField, validateRequiredFields } from './forms';

export class SetupRclone extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["remotePath"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "Rclone Remote Path", "remotePath", { autoFocus: true, placeholder: "enter <name-of-rclone-remote>:<path>" })}
            </Row>
            <Row>
                {OptionalField(this, "rclone executable", "rcloneExe", { placeholder: "enter path to rclone executable" })}
            </Row>
        </>;
    }
}
