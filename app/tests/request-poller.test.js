import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import { createRequestPoller } from "../public/request-poller.js";

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(promise) {
  let timeout;

  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error("timed out")), 1000);
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

test("timed-out polls are destroyed and never overlap", async () => {
  let activeRequests = 0;
  let destroyedRequests = 0;
  let maxActiveRequests = 0;
  let requestCount = 0;
  let enoughRequestsResolve;
  const enoughRequests = new Promise((resolve) => {
    enoughRequestsResolve = resolve;
  });

  const poller = createRequestPoller({
    interval: 5,
    getRequestOptions: () => ({ timeout: 5 }),
    request: (options) => {
      const req = new EventEmitter();
      let destroyed = false;

      requestCount++;
      if (requestCount === 3) {
        enoughRequestsResolve();
      }

      activeRequests++;
      maxActiveRequests = Math.max(maxActiveRequests, activeRequests);

      req.end = () => {
        setTimeout(() => req.emit("timeout"), options.timeout);
      };
      req.destroy = () => {
        if (!destroyed) {
          destroyed = true;
          destroyedRequests++;
          activeRequests--;
        }
      };

      return req;
    },
    onResponse: () => assert.fail("unexpected response"),
    onError: (err) => assert.fail(err),
  });

  poller.start();
  await waitFor(enoughRequests);
  poller.stop();

  assert.equal(requestCount, 3);
  assert.equal(maxActiveRequests, 1);
  assert.equal(destroyedRequests, requestCount);
  assert.equal(activeRequests, 0);
});

test("stopping destroys a hung poll and prevents another poll", async () => {
  let requestCount = 0;
  let destroyedRequests = 0;
  let requestStartedResolve;
  const requestStarted = new Promise((resolve) => {
    requestStartedResolve = resolve;
  });

  const poller = createRequestPoller({
    interval: 5,
    getRequestOptions: () => ({ timeout: 60_000 }),
    request: () => {
      const req = new EventEmitter();

      requestCount++;
      requestStartedResolve();
      req.end = () => {};
      req.destroy = () => destroyedRequests++;

      return req;
    },
    onResponse: () => assert.fail("unexpected response"),
    onError: (err) => assert.fail(err),
  });

  poller.start();
  await waitFor(requestStarted);
  poller.stop();
  await delay(15);

  assert.equal(requestCount, 1);
  assert.equal(destroyedRequests, 1);
});

test("response bodies are collected before the next poll", async () => {
  let requestCount = 0;
  let receivedBody;
  let poller;
  let responseReceivedResolve;
  const responseReceived = new Promise((resolve) => {
    responseReceivedResolve = resolve;
  });

  poller = createRequestPoller({
    interval: 5,
    getRequestOptions: () => ({}),
    request: (_options, handleResponse) => {
      const req = new EventEmitter();

      requestCount++;
      req.end = () => {
        queueMicrotask(() => {
          const resp = new EventEmitter();
          resp.statusCode = 200;
          handleResponse(resp);
          resp.emit("data", Buffer.from('{"connected":'));
          resp.emit("data", Buffer.from("false}"));
          resp.emit("end");
        });
      };
      req.destroy = () => {};

      return req;
    },
    onResponse: (_resp, body) => {
      receivedBody = body.toString();
      poller.stop();
      responseReceivedResolve();
    },
    onError: (err) => assert.fail(err),
  });

  poller.start();
  await waitFor(responseReceived);

  assert.equal(requestCount, 1);
  assert.equal(receivedBody, '{"connected":false}');
});
