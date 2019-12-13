import React, { Component } from 'react';
import ReactTable from 'react-table';
import { Link } from "react-router-dom";

import {
    sizeDisplayName,
    objectLink,
} from './uiutil';

function objectName(name, typeID) {
    if (typeID === "d") {
        return name + "/";
    }

    return name
}

function sizeInfo(item) {
    if (item.size) {
        return sizeDisplayName(item.size);
    }
    const summ = item.summ;
    if (!summ) {
        return "";
    }
    return sizeDisplayName(summ.size) + ", " + summ.files + " files, " + summ.dirs + " dirs";
}

function sizeForSorting(item) {
    if (item.size) {
        return item.size;
    }

    if (item.summ && item.summ.size) {
        return item.summ.size;
    }

    return 0;
}

function sizeSortMethod(a, b, desc) {
    const l = sizeForSorting(a);
    const r = sizeForSorting(b);

    if (l < r) {
        return -1;
    }
    if (l > r) {
        return 1;
    }
    return 0;
}

function directoryLinkOrDownload(x) {
    if (x.obj.startsWith("k")) {
        return <Link to={objectLink(x.obj)}>{objectName(x.name, x.type)}</Link>;
    }

    return <a href={"/api/v1/objects/" + x.obj + "?fname=" + x.name}>{x.name}</a>;
}

export class DirectoryItems extends Component {
    render() {
        const columns = [{
            id: "name",
            Header: 'Name',
            accessor: x => directoryLinkOrDownload(x),
        }, {
            id: "mtime",
            accessor: "mtime",
            Header: "Last Mod",
        }, {
            id: "size",
            accessor: x => sizeInfo(x),
            Header: "Size",
            sortMethod: sizeSortMethod,
        }]

        return <ReactTable data={this.props.items} columns={columns} />;
    }
}
