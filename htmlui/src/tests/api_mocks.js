import axios from 'axios';
import MockAdapter from 'axios-mock-adapter';

export function setupAPIMock() {
    let axiosMock = new MockAdapter(axios);
    axiosMock.reset();

    axiosMock.onGet("/api/v1/repo/algorithms").reply(200, {
        defaultEncryption: "e-bar",
        encryption: ["e-foo", "e-bar", "e-baz"],

        defaultSplitter: "s-bar",
        splitter: ["s-foo", "s-bar", "s-baz"],

        defaultHash: "h-bar",
        hash: ["h-foo", "h-bar", "h-baz"],

        compression: ["c-foo", "c-bar", "c-baz"],
    });

    return axiosMock;
}
