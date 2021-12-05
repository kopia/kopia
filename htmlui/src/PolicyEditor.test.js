import { render, waitFor, logDOM } from '@testing-library/react';
import React from 'react';
import { PolicyEditor } from './PolicyEditor';
import { MemoryRouter } from 'react-router-dom';
import { setupAPIMock } from './tests/api_mocks';
import moment from 'moment';
import { changeControlValue, simulateClick } from './tests/testutils';

it('e2e', async () => {
    let ref = React.createRef();
    let serverMock = setupAPIMock();

    const ust1 = "2021-01-01T12:00:00Z";
    const ust2 = "2021-01-01T13:00:00Z";

    // first request from client - will fetch current policy
    serverMock.onGet('/api/v1/policy?userName=some-user&host=some-host&path=some-path').reply(200,
        {
            "retention": {
                "keepLatest": 33,
            }
        }
    );

    // second request to resolve the policy.
    serverMock.onPost('/api/v1/policy/resolve?userName=some-user&host=some-host&path=some-path', {
        "updates": {
            "retention": {
                "keepLatest": 33
            }
        },
        "numUpcomingSnapshotTimes": 5
    }).reply(200,
        {
            "effective": {
                "retention": {
                    "keepLatest": 33,
                    "keepHourly": 45,
                }
            },
            "definition": {
                "retention": {
                    "keepLatest": { "host": "h1", "userName": "some-user", "path": "some-path" },
                    "keepMonthly": { "host": "h1", "userName": "u1", "path": "p1" }
                }
            },
            "upcomingSnapshotTimes": [
                ust1,
                ust2,
            ]
        }
    );

        // second request to resolve the policy.
        serverMock.onPost('/api/v1/policy/resolve?userName=some-user&host=some-host&path=some-path', {
            "updates": {
                "retention": {
                    "keepLatest": 44
                }
            },
            "numUpcomingSnapshotTimes": 5
        }).reply(200,
            {
                "effective": {
                    "retention": {
                        "keepLatest": 44,
                        "keepHourly": 45,
                    }
                },
                "definition": {
                    "retention": {
                        "keepLatest": { "host": "some-host", "userName": "some-user", "path": "some-path" },
                        "keepMonthly": { "host": "h1", "userName": "u1", "path": "p1" }
                    }
                },
                "upcomingSnapshotTimes": [
                    ust1,
                    ust2,
                ]
            }
        );

    // request to save updated policy.
    serverMock.onPut('/api/v1/policy?userName=some-user&host=some-host&path=some-path', {
        "retention": {
            "keepLatest": 44
        }
    }).reply(200,
        {
            "effective": {
                "retention": {
                    "keepLatest": 33,
                    "keepHourly": 45,
                }
            },
            "definition": {
                "retention": {
                    "keepLatest": { "host": "h1", "userName": "some-user", "path": "some-path" },
                    "keepMonthly": { "host": "h1", "userName": "u1", "path": "p1" }
                }
            },
            "upcomingSnapshotTimes": [
                ust1,
                ust2,
            ]
        }
    );

    let closeCalled = 0;

    function closeFunc() {
        closeCalled++;
    }
    
    const { getByTestId } = render(<MemoryRouter>
        <PolicyEditor ref={ref} host="some-host" userName="some-user" path="some-path" close={closeFunc} />
    </MemoryRouter>)

    await waitFor(() => expect(getByTestId("effective-retention.keepHourly").value).toBe("45"));
    expect(getByTestId("effective-retention.keepLatest").value).toEqual("33");
    if (false) {
        logDOM(getByTestId("definition-retention.keepHourly"));
    }
    
    await waitFor(() => expect(getByTestId("definition-retention.keepLatest").innerHTML).toContain(`Directory: some-user@h1:some-path`));
    await waitFor(() => expect(getByTestId("definition-retention.keepMonthly").innerHTML).toContain("Directory: u1@h1:p1"));

    const expectedUpcomingSnapshotTimes = [
        `<li>${moment(ust1).format('L LT')} (${moment(ust1).fromNow()})</li>`,
        `<li>${moment(ust2).format('L LT')} (${moment(ust2).fromNow()})</li>`,
    ].join("");

    await waitFor(() => expect(getByTestId("upcoming-snapshot-times").innerHTML).toEqual(expectedUpcomingSnapshotTimes));

    // change a field
    changeControlValue(getByTestId('control-policy.retention.keepLatest'), "44");

    // this will trigger resolve and will update effective field: "(Defined by this policy)"
    await waitFor(() => expect(getByTestId("effective-retention.keepLatest").value).toBe("44"));
    await waitFor(() => expect(getByTestId("definition-retention.keepLatest").innerHTML).toEqual("(Defined by this policy)"));

    simulateClick(getByTestId('button-save'));
    await waitFor(() => expect(serverMock.history.put.length).toEqual(1));
    await waitFor(() => expect(closeCalled).toEqual(1));
});
