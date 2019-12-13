import React, { Component } from 'react';
import axios from 'axios';

import { DirectoryItems } from "./DirectoryItems";
import Button from 'react-bootstrap/Button';
import Spinner from 'react-bootstrap/Spinner';

export class DirectoryObject extends Component {
    constructor() {
        super();

        this.state = {
            items: [],
            isLoading: false,
            error: null,
        };
    }

    componentDidMount() {
        this.fetchDirectory(this.props);
    }

    fetchDirectory(props) {
        console.log('fetching props:', props);
        let oid = props.match.params.oid;

        this.setState({
            isLoading: true,
        });
        axios.get('/api/v1/objects/' + oid).then(result => {
            this.setState({
                items: result.data.entries,
                isLoading: false,
            });
        }).catch(error => this.setState({
            error,
            isLoading: false
        }));
    }

    componentWillReceiveProps(props) {
        this.fetchDirectory(props);
    }

    render() {
        let { items, isLoading, error } = this.state;
        if (error) {
            return <p>ERROR: {error.message}</p>;
        }
        if (isLoading) {
            return <Spinner animation="border" variant="primary" />;
        }

        return <div>
            <Button size="xxl" onClick={this.props.history.goBack} >
                Back
            </Button>
            <DirectoryItems items={items} />
        </div>
    }
}
