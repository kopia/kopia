import React from 'react';
import { getDeepStateProperty, setDeepStateProperty } from '../deepstate';

it('can get fields', async () => {
    const comp = {
        "state": {
            "a": {
                "b": {
                    "c": {
                        "d": 1,
                        "e": "eee",
                        "f": true,
                        "g": [1, 2, 3]
                    }
                }
            }
        }
    }

    expect(getDeepStateProperty(comp, "a.b.c.d")).toStrictEqual(1);
    expect(getDeepStateProperty(comp, "a")).toStrictEqual({
        "b": {
            "c": {
                "d": 1,
                "e": "eee",
                "f": true,
                "g": [1, 2, 3]
            }
        }
    });

    expect(getDeepStateProperty(comp, "a.b")).toStrictEqual({
        "c": {
            "d": 1,
            "e": "eee",
            "f": true,
            "g": [1, 2, 3]
        }
    });

    expect(getDeepStateProperty(comp, "a.b.c")).toStrictEqual({
        "d": 1,
        "e": "eee",
        "f": true,
        "g": [1, 2, 3]
    });

    expect(getDeepStateProperty(comp, "a.b.x")).toStrictEqual("");
    expect(getDeepStateProperty(comp, "a.b.x", "defaultValue")).toStrictEqual("defaultValue");
    expect(getDeepStateProperty(comp, "a.b.x", 3.1415)).toStrictEqual(3.1415);
});

it('can set fields', async () => {
    let lastSetState;

    const comp = {
        "setState": function (s) {
            lastSetState = s
        },
        "state": {
            "a": {
                "b": {
                    "c": {
                        "d": 1,
                    }
                }
            }
        }
    }

    setDeepStateProperty(comp, "a.b.x", 42);
    expect(lastSetState).toStrictEqual({
        "a": {
            "b": {
                "c": {
                    "d": 1,
                },
                "x": 42,
            }
        }
    });
    setDeepStateProperty(comp, "a.b.r.y.z", 42);
    expect(lastSetState).toStrictEqual({
        "a": {
            "b": {
                "c": {
                    "d": 1,
                },
                "r": {
                    "y": {
                        "z": 42
                    }
                }
            }
        }
    });
    setDeepStateProperty(comp, "a.b", undefined);
    expect(lastSetState).toStrictEqual({
        "a": {
            "b": undefined,
        }
    });
});
