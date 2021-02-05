
import { faChevronLeft, faStopCircle } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Alert from 'react-bootstrap/Alert';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import { TaskLogs } from './TaskLogs';
import { cancelTask, redirectIfNotConnected, taskStatusSymbol } from './uiutil';

export class TaskDetails extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: true,
            error: null,
        };

        this.fetchTask = this.fetchTask.bind(this);
        this.interval = window.setInterval(() => this.fetchTask(this.props), 3000);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchTask(this.props);
    }

    componentWillUnmount() {
        window.clearInterval(this.interval);
    }

    fetchTask(props) {
        let tid = props.match.params.tid;

        axios.get('/api/v1/tasks/' + tid).then(result => {
            this.setState({
                task: result.data,
                isLoading: false,
            });
        }).catch(error => {
            redirectIfNotConnected(error);
            this.setState({
                error,
                isLoading: false
            });
        });
    }

    componentWillReceiveProps(props) {
        this.fetchTask(props);
    }

    render() {
        const { task, isLoading, error } = this.state;
        if (error) {
            return <p>{error.message}</p>;
        }

        if (isLoading) {
            return <p>Loading ...</p>;
        }

        return <Form>
            <Form.Row>
                <Col xs={6}>
                <Form.Group>
                    <Button size="sm" variant="outline-secondary" onClick={this.props.history.goBack} ><FontAwesomeIcon icon={faChevronLeft} /> Return </Button>
                    {task.status === "RUNNING" && <>
                        &nbsp;<Button size="sm" variant="danger" onClick={() => cancelTask(task.id)} ><FontAwesomeIcon icon={faStopCircle} /> Stop </Button>
                    </>}
                    </Form.Group>
                    </Col>
                    <Col xs={4}>
                        Started at <b>{new Date(task.startTime).toLocaleString()}</b>
                    </Col>
                    <Col xs={2}>
                        {taskStatusSymbol(task)}
                    </Col>
            </Form.Row>
            <Form.Row>
                <Col xs={3} >
                    <Form.Group>
                        <Form.Label>Kind</Form.Label>
                        <Form.Control type="text" readOnly={true} value={task.kind} />
                    </Form.Group>
                </Col>
                <Col xs={9} >
                    <Form.Group>
                        <Form.Label>Description</Form.Label>
                        <Form.Control type="text" readOnly={true} value={task.description} />
                    </Form.Group>
                </Col>
            </Form.Row>
            {task.errorMessage && <Form.Row>
                <Col>
                    <Alert variant="danger">Task failed with: {task.errorMessage}</Alert>
                </Col>
            </Form.Row>}
            <Form.Row>
                <Col>
                    <TaskLogs taskID={this.props.match.params.tid} />
                </Col>
            </Form.Row>
        </Form>
            ;
    }
}
