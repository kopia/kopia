import { test, expect } from "@playwright/test";

import {
  applySnapshotReport,
  localSourceStatuses,
  parseSnapshotReport,
  pruneFailures,
  shortenPath,
  sourceStatusLabel,
  trayIconState,
  traySummary,
  trayTitle,
  trayToolTip,
  uploadPercent,
} from "../public/tray-status.js";

function sourceStatus(path, status, upload, userName = "me", host = "box") {
  return { source: { host, userName, path }, status, upload };
}

// matches the default snapshot-report.txt notification template output.
const snapshotReportBody = `Path: /home/me/docs

  Status:      fatal
  Start:       Thu, 01 Jan 1970 00:00:00 +0000
  Duration:    0s
  Size:        0 B
  Files:       0
  Directories: 0
  Error:       some top-level error

Path: /home/me/pics

  Status:      success
  Start:       Thu, 02 Jan 2020 03:04:05 +0000
  Duration:    1.1s
  Size:        456 B (+56 B)
  Files:       123 (+23)
  Directories: 33 (+3)

Path: /home/me/music

  Status:      warnings
  Start:       Thu, 02 Jan 2020 03:04:05 +0000

  Failed Entries:

  - /home/me/music/locked.mp3: permission denied

Path: /home/me/video

  Status:      incomplete
  Start:       Thu, 02 Jan 2020 03:04:05 +0000

Generated at Thu, 02 Jan 2020 03:04:05 +0000 by Kopia v0.23.0.
`;

test("uploadPercent", () => {
  expect(uploadPercent(0, 0)).toBeNull();
  expect(uploadPercent(50, 0)).toBeNull();
  expect(uploadPercent(0, 200)).toBe(0);
  expect(uploadPercent(199, 200)).toBe(99);
  expect(uploadPercent(200, 200)).toBe(100);
  // estimate exceeded, percent would be misleading
  expect(uploadPercent(300, 200)).toBeNull();
});

test("localSourceStatuses keeps only local sources", () => {
  const resp = {
    localUsername: "me",
    localHost: "box",
    sources: [
      sourceStatus("/home/me/docs", "UPLOADING", {
        hashedBytes: 30,
        cachedBytes: 20,
        estimatedBytes: 200,
      }),
      sourceStatus("/home/me/pics", "IDLE"),
      sourceStatus("/home/other", "REMOTE", undefined, "other"),
      sourceStatus("/home/me/docs", "REMOTE", undefined, "me", "laptop"),
    ],
  };

  expect(localSourceStatuses(resp)).toStrictEqual([
    {
      path: "/home/me/docs",
      status: "UPLOADING",
      doneBytes: 50,
      estimatedBytes: 200,
      percent: 25,
    },
    {
      path: "/home/me/pics",
      status: "IDLE",
      doneBytes: 0,
      estimatedBytes: 0,
      percent: null,
    },
  ]);
});

test("localSourceStatuses handles empty or missing responses", () => {
  expect(localSourceStatuses(undefined)).toStrictEqual([]);
  expect(localSourceStatuses({})).toStrictEqual([]);
  expect(
    localSourceStatuses({ localUsername: "me", localHost: "box", sources: [] }),
  ).toStrictEqual([]);
});

test("sourceStatusLabel", () => {
  expect(sourceStatusLabel({ status: "UPLOADING", percent: 42 })).toBe("42%");
  expect(sourceStatusLabel({ status: "UPLOADING", percent: 0 })).toBe("0%");
  expect(sourceStatusLabel({ status: "UPLOADING", percent: null })).toBe(
    "Uploading",
  );
  expect(sourceStatusLabel({ status: "IDLE", percent: null })).toBe("Idle");
  expect(sourceStatusLabel({ status: "PENDING", percent: null })).toBe(
    "Pending",
  );
  expect(sourceStatusLabel({ status: "PAUSED", percent: null })).toBe("Paused");
  expect(sourceStatusLabel({ status: "FAILED", percent: null })).toBe("Failed");
  // unknown statuses from newer servers are shown as-is
  expect(sourceStatusLabel({ status: "NEW_STATE", percent: null })).toBe(
    "NEW_STATE",
  );
});

test("sourceStatusLabel shows last snapshot failure while idle", () => {
  expect(
    sourceStatusLabel({ status: "IDLE", percent: null, failure: "fatal" }),
  ).toBe("Failed");
  expect(
    sourceStatusLabel({ status: "IDLE", percent: null, failure: "warnings" }),
  ).toBe("Errors");
  // a running snapshot is shown instead of the previous failure
  expect(
    sourceStatusLabel({ status: "UPLOADING", percent: 10, failure: "fatal" }),
  ).toBe("10%");
  expect(
    sourceStatusLabel({ status: "PENDING", percent: null, failure: "fatal" }),
  ).toBe("Pending");
});

test("parseSnapshotReport", () => {
  expect(parseSnapshotReport(snapshotReportBody)).toStrictEqual([
    { path: "/home/me/docs", status: "fatal" },
    { path: "/home/me/pics", status: "success" },
    { path: "/home/me/music", status: "warnings" },
    { path: "/home/me/video", status: "incomplete" },
  ]);
  // other notifications, such as maintenance errors, have no per-source results
  expect(parseSnapshotReport("Operation: Maintenance\nError: boom")).toEqual(
    [],
  );
  expect(parseSnapshotReport(undefined)).toEqual([]);
});

test("applySnapshotReport records failures until that source succeeds", () => {
  let failures = applySnapshotReport(
    {},
    parseSnapshotReport(snapshotReportBody),
  );
  expect(failures).toStrictEqual({
    "/home/me/docs": "fatal",
    "/home/me/music": "warnings",
  });

  // success of another source leaves the failure in place
  failures = applySnapshotReport(failures, [
    { path: "/home/me/pics", status: "success" },
  ]);
  expect(failures["/home/me/docs"]).toBe("fatal");

  // incomplete (cancelled) snapshot neither sets nor clears
  failures = applySnapshotReport(failures, [
    { path: "/home/me/docs", status: "incomplete" },
  ]);
  expect(failures["/home/me/docs"]).toBe("fatal");

  failures = applySnapshotReport(failures, [
    { path: "/home/me/docs", status: "success" },
  ]);
  expect(failures).toStrictEqual({ "/home/me/music": "warnings" });
});

test("parseSnapshotReport finds nothing in a customised template", () => {
  expect(parseSnapshotReport("Source: /home/me/docs\nResult: fatal\n")).toEqual(
    [],
  );
});

test("pruneFailures drops removed sources and repositories", () => {
  const failures = {
    repo1: { "/a": "fatal", "/removed": "warnings" },
    repo2: { "/b": "fatal" },
    deletedRepo: { "/c": "fatal" },
  };

  expect(
    pruneFailures(failures, {
      repo1: ["/a", "/other"],
      // sources not known yet, keep as-is
      repo2: null,
    }),
  ).toStrictEqual({
    repo1: { "/a": "fatal" },
    repo2: { "/b": "fatal" },
  });

  expect(pruneFailures(failures, { repo1: ["/other"] })).toStrictEqual({});

  // last source removed: a known empty list clears the repository's failures
  expect(pruneFailures(failures, { repo1: [], repo2: null })).toStrictEqual({
    repo2: { "/b": "fatal" },
  });
});

test("applySnapshotReport does not modify its input", () => {
  const failures = { "/a": "fatal" };
  applySnapshotReport(failures, [{ path: "/a", status: "success" }]);
  expect(failures).toStrictEqual({ "/a": "fatal" });
});

test("shortenPath replaces home directory", () => {
  expect(shortenPath("/Users/me", "/Users/me")).toBe("~");
  expect(shortenPath("/Users/me/Documents", "/Users/me")).toBe("~/Documents");
  // shared prefix only, not the home directory
  expect(shortenPath("/Users/me2/Documents", "/Users/me")).toBe(
    "/Users/me2/Documents",
  );
  // empty home directory disables the substitution (used on Windows)
  expect(shortenPath("C:\\Users\\me\\Documents", "")).toBe(
    "C:\\Users\\me\\Documents",
  );
});

test("shortenPath trims long paths in the middle", () => {
  const p = "/very/long/path/that/goes/on/and/on/to/a/folder";
  const short = shortenPath(p, "/Users/me", 20);

  expect(short).toHaveLength(20);
  expect(short).toBe("/very/lon...a/folder");
  expect(shortenPath("/short/path", "/Users/me", 20)).toBe("/short/path");
});

test("traySummary weights percent by estimated size", () => {
  const statuses = [
    {
      status: "UPLOADING",
      doneBytes: 100,
      estimatedBytes: 100,
      percent: 100,
    },
    { status: "UPLOADING", doneBytes: 0, estimatedBytes: 300, percent: 0 },
    // no estimate yet, counted as uploading but excluded from percent
    { status: "UPLOADING", doneBytes: 50, estimatedBytes: 0, percent: null },
    { status: "IDLE", doneBytes: 0, estimatedBytes: 0, percent: null },
  ];

  expect(traySummary(statuses)).toStrictEqual({
    uploading: 3,
    percent: 25,
    failed: 0,
  });
});

test("traySummary counts sources with failures", () => {
  expect(traySummary([])).toStrictEqual({
    uploading: 0,
    percent: null,
    failed: 0,
  });
  expect(
    traySummary([
      { status: "IDLE", doneBytes: 0, estimatedBytes: 0, percent: null },
      {
        status: "IDLE",
        doneBytes: 0,
        estimatedBytes: 0,
        percent: null,
        failure: "fatal",
      },
    ]),
  ).toStrictEqual({ uploading: 0, percent: null, failed: 1 });
});

test("trayIconState", () => {
  expect(trayIconState({ uploading: 0, failed: 0 })).toBe("idle");
  expect(trayIconState({ uploading: 0, failed: 2 })).toBe("error");
  // a running upload takes priority over earlier failures
  expect(trayIconState({ uploading: 1, failed: 2 })).toBe("busy");
});

test("trayTitle", () => {
  expect(trayTitle({ uploading: 0, percent: null })).toBe("");
  expect(trayTitle({ uploading: 1, percent: null })).toBe("");
  expect(trayTitle({ uploading: 2, percent: 42 })).toBe("42%");
});

test("trayToolTip", () => {
  expect(trayToolTip({ uploading: 0, percent: null, failed: 0 })).toBe("Kopia");
  expect(trayToolTip({ uploading: 1, percent: null, failed: 0 })).toBe(
    "Kopia - 1 snapshot in progress",
  );
  expect(trayToolTip({ uploading: 2, percent: 42, failed: 0 })).toBe(
    "Kopia - 2 snapshots in progress (42%)",
  );
  expect(trayToolTip({ uploading: 0, percent: null, failed: 1 })).toBe(
    "Kopia - 1 source with errors",
  );
  expect(trayToolTip({ uploading: 1, percent: 5, failed: 2 })).toBe(
    "Kopia - 1 snapshot in progress (5%), 2 sources with errors",
  );
});
