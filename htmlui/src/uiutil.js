import React from 'react';
import Button from 'react-bootstrap/Button';
import ListGroup from 'react-bootstrap/ListGroup';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Popover from 'react-bootstrap/Popover';

const base10UnitPrefixes = ["", "K", "M", "G", "T"];

function niceNumber(f) {
    return (Math.round(f * 10) / 10.0) + '';
}

function toDecimalUnitString(f, thousand, prefixes, suffix) {
    for (var i = 0; i < prefixes.length; i++) {
        if (f < 0.9 * thousand) {
            return niceNumber(f) + ' ' + prefixes[i] + suffix;
        }
        f /= thousand
    }

    return niceNumber(f) + ' ' + prefixes[prefixes.length - 1] + suffix;
}

export function sizeWithFailures(size, summ) {
    if (size === undefined) {
        return "";
    }

    if (!summ || !summ.errors || !summ.numFailed) {
        return <span>{sizeDisplayName(size)}</span>
    }

    let caption = summ.numFailed + " Errors";
    if (summ.numFailed === 1) {
        caption = "Error";
    }

    let overlay = <Popover id="popover-basic">
        <Popover.Title as="h3">{caption} During Snapshot</Popover.Title>
        <Popover.Content>
            <ListGroup>
                {summ.errors.map(x => <ListGroup.Item key={x.path}>Path: <code>{x.path}</code><br />Error: <span className="error">{x.error}</span></ListGroup.Item>)}
            </ListGroup>
        </Popover.Content>
    </Popover>;

    return <span>
        {sizeDisplayName(size)}<br />
        <OverlayTrigger placement="bottom"
            delay={{ show: 0, hide: 1000 }}
            overlay={overlay}>
            <Button size="sm" variant="danger">{caption}</Button>
        </OverlayTrigger>
    </span>;
}

export function sizeDisplayName(s) {
    if (s === undefined) {
        return "";
    }
    return toDecimalUnitString(s, 1000, base10UnitPrefixes, "B");
}

export function intervalDisplayName(v) {
    return "-";
}

export function timesOfDayDisplayName(v) {
    if (!v) {
        return "(none)";
    }
    return v.length + " times";
}

export function parseQuery(queryString) {
    var query = {};
    var pairs = (queryString[0] === '?' ? queryString.substr(1) : queryString).split('&');
    for (var i = 0; i < pairs.length; i++) {
        var pair = pairs[i].split('=');
        query[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1] || '');
    }
    return query;
}

export function rfc3339TimestampForDisplay(n) {
    if (!n) {
        return "";
    }

    let t = new Date(n);
    return t.toLocaleString();
}

export function objectLink(n) {
    if (n.startsWith("k")) {
        return "/snapshots/dir/" + n;
    }
    return "/api/v1/objects/" + n;
}

export function ownerName(s) {
    return s.userName + "@" + s.host;
}

export function compare(a, b) {
    return (a < b ? -1 : (a > b ? 1 : 0));
}

export function redirectIfNotConnected(e) {
    if (e && e.response && e.response.data && e.response.data.code === "NOT_CONNECTED") {
        window.location.replace("/repo");
        return;
    }
}