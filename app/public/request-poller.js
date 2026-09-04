// Creates a polling loop that never has more than one request in flight.
export function createRequestPoller({
  interval,
  request,
  getRequestOptions,
  onResponse,
  onError,
}) {
  let activeRequest = null;
  let pollTimer = null;
  let stopped = true;

  function scheduleNextPoll() {
    if (!stopped) {
      pollTimer = setTimeout(pollOnce, interval);
    }
  }

  function finishRequest(req, callback) {
    if (activeRequest !== req) {
      return;
    }

    activeRequest = null;

    try {
      callback?.();
    } finally {
      scheduleNextPoll();
    }
  }

  function pollOnce() {
    pollTimer = null;

    if (stopped) {
      return;
    }

    const options = getRequestOptions();
    if (!options) {
      scheduleNextPoll();
      return;
    }

    let req;

    try {
      req = request(options, (resp) => {
        const chunks = [];

        resp.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        resp.once("end", () => {
          finishRequest(req, () => onResponse(resp, Buffer.concat(chunks)));
        });
        resp.once("aborted", () => {
          finishRequest(req, () => onError(new Error("response aborted")));
        });
        resp.once("error", (err) => {
          finishRequest(req, () => onError(err));
        });
      });
    } catch (err) {
      try {
        onError(err);
      } finally {
        scheduleNextPoll();
      }

      return;
    }

    activeRequest = req;

    req.once("timeout", () => {
      if (activeRequest !== req) {
        return;
      }

      // A ClientRequest timeout is only a notification. Explicitly destroy the
      // request so its socket is not retained while the next poll is scheduled.
      req.destroy();
      finishRequest(req);
    });
    req.once("error", (err) => {
      finishRequest(req, () => onError(err));
    });

    try {
      req.end();
    } catch (err) {
      req.destroy();
      finishRequest(req, () => onError(err));
    }
  }

  return {
    start() {
      if (!stopped) {
        return;
      }

      stopped = false;
      scheduleNextPoll();
    },

    stop() {
      stopped = true;

      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }

      const req = activeRequest;
      activeRequest = null;
      req?.destroy();
    },
  };
}
