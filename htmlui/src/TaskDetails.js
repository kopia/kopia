
import { faChevronCircleDown, faChevronCircleUp, faStopCircle } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import React, { Component } from 'react';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import { TaskLogs } from './TaskLogs';
import { cancelTask, formatDuration, GoBackButton, redirectIfNotConnected, sizeDisplayName } from './uiutil';

export class TaskDetails extends Component {
    constructor() {
        super();
        this.state = {
            items: [],
            isLoading: true,
            error: null,
            showLog: false,
        };

        this.taskID = this.taskID.bind(this);
        this.fetchTask = this.fetchTask.bind(this);

        // poll frequently, we will stop as soon as the task ends.
        this.interval = window.setInterval(() => this.fetchTask(this.props), 500);
    }

    componentDidMount() {
        this.setState({
            isLoading: true,
        });

        this.fetchTask(this.props);
    }

    componentWillUnmount() {
        if (this.interval) {
            window.clearInterval(this.interval);
        }
    }

    taskID(props) {
        return props.taskID || props.match.params.tid;
    }

    fetchTask(props) {
        axios.get('/api/v1/tasks/' + this.taskID(props)).then(result => {
            this.setState({
                task: result.data,
                isLoading: false,
            });

            if (result.data.endTime) {
                window.clearInterval(this.interval);
                this.interval = null;
            }
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

    summaryControl(task) {
        const dur = formatDuration(task.startTime, task.endTime)

        switch (task.status) {

            case "SUCCESS":
                return <Alert variant="success">Task succeeded after {dur}.</Alert>;

            case "FAILED":
                return <Alert variant="danger"><b>Error:</b> {task.errorMessage}.</Alert>;

            case "CANCELED":
                return <Alert variant="warning">Task canceled.</Alert>;

            case "CANCELING":
                return <Alert variant="primary">
                    <Spinner animation="border" variant="warning" size="sm" /> Canceling {dur}: {task.progressInfo}.</Alert>;

            default:
                return <Alert variant="primary">
                    <Spinner animation="border" variant="primary" size="sm" /> Running for {dur}: {task.progressInfo}.</Alert>;
        }
    }

    valueThreshold() {
        if (this.props.showZeroCounters) {
            return -1;
        }

        return 0
    }

    counterBadge(label, c) {
        if (c.value < this.valueThreshold()) {
            return "";
        }

        let formatted = c.value.toLocaleString();
        if (c.units === "bytes") {
            formatted = sizeDisplayName(c.value);
        }

        let variant = "secondary";
        switch (c.level) {
            case "warning":
                variant = "warning";
                break;
            case "error":
                variant = "danger";
                break;
            case "notice":
                variant = "info";
                break;
            default:
                variant = "secondary";
                break;
        }

        return <Badge key={label} className="counter-badge" bg={variant}>{label}: {formatted}</Badge>
    }

    counterLevelToSortOrder(l) {
        switch (l) {
            case "error":
                return 30
            case "notice":
                return 10;
            case "warning":
                return 5;
            default:
                return 0;
        }
    }

    sortedBadges(counters) {
        let keys = Object.keys(counters);

        // sort keys by their level and the name alphabetically.
        keys.sort((a, b) => {
            if (counters[a].level !== counters[b].level) {
                return this.counterLevelToSortOrder(counters[b].level) - this.counterLevelToSortOrder(counters[a].level);
            }

            if (a < b) {
                return -1;
            }

            if (a > b) {
                return 1;
            }

            return 0;
        });

        return keys.map(c => (counters[c].value > this.valueThreshold()) && this.counterBadge(c, counters[c]));
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
            {this.props.history &&
                <Row>
                    <Form.Group>
                        <GoBackButton onClick={this.props.history.goBack} />
                        {task.status === "RUNNING" && <>
                            &nbsp;<Button size="sm" variant="danger" onClick={() => cancelTask(task.id)} ><FontAwesomeIcon icon={faStopCircle} /> Stop </Button>
                        </>}
                    </Form.Group>
                </Row>}
            {!this.props.hideDescription && <Row>
                <Col xs={3} >
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={task.kind} />
                    </Form.Group>
                </Col>
                <Col xs={9} >
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={task.description} />
                    </Form.Group>
                </Col>
            </Row>}
            <Row>
                <Col xs={9}>
                    {this.summaryControl(task)}
                </Col>
                <Col xs={3}>
                    <Form.Group>
                        <Form.Control type="text" readOnly={true} value={"Started: " + new Date(task.startTime).toLocaleString()} />
                    </Form.Group>
                </Col>
            </Row>
            {task.counters && <Row>
                <Col>
                    {this.sortedBadges(task.counters)}
                </Col>
            </Row>}
            <hr />
            <Row>
                <Col>
                    {this.state.showLog ? <>
                        <Button size="sm" onClick={() => this.setState({ showLog: false })}><FontAwesomeIcon icon={faChevronCircleUp} /> Hide Log</Button>
                        <TaskLogs taskID={this.taskID(this.props)} />
                    </> : <Button size="sm" onClick={() => this.setState({ showLog: true })}><FontAwesomeIcon icon={faChevronCircleDown} /> Show Log</Button>}
                </Col>
            </Row>
        </Form>
            ;
    }
}
