import React, { Component } from 'react';
import Row from 'react-bootstrap/Row';
import { handleChange, RequiredField, validateRequiredFields } from './forms';


export class SetupFilesystem extends Component {
    constructor(props) {
        super();

        this.state = {
            ...props.initial
        };
        this.handleChange = handleChange.bind(this);
    }

    validate() {
        return validateRequiredFields(this, ["path"])
    }

    render() {
        return <>
            <Row>
                {RequiredField(this, "Directory Path", "path", { autoFocus: true, placeholder: "enter path to a directory where to store repository files" })}
            </Row>
        </>;
    }
}
