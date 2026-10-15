import { test, expect } from "@playwright/test";

import {
  localSourceStatuses,
  overallProgress,
  shortenPath,
  sourceStatusLabel,
  trayTitle,
  trayToolTip,
  uploadPercent,
} from "../public/tray-status.js";

function sourceStatus(path, status, upload, userName = "me", host = "box") {
  return { source: { host, userName, path }, status, upload };
}

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

test("overallProgress weights by estimated size", () => {
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

  expect(overallProgress(statuses)).toStrictEqual({
    uploading: 3,
    percent: 25,
  });
});

test("overallProgress with nothing uploading", () => {
  expect(overallProgress([])).toStrictEqual({ uploading: 0, percent: null });
  expect(
    overallProgress([
      { status: "IDLE", doneBytes: 0, estimatedBytes: 0, percent: null },
    ]),
  ).toStrictEqual({ uploading: 0, percent: null });
});

test("trayTitle", () => {
  expect(trayTitle({ uploading: 0, percent: null })).toBe("");
  expect(trayTitle({ uploading: 1, percent: null })).toBe("...");
  expect(trayTitle({ uploading: 2, percent: 42 })).toBe("42%");
});

test("trayToolTip", () => {
  expect(trayToolTip({ uploading: 0, percent: null })).toBe("Kopia");
  expect(trayToolTip({ uploading: 1, percent: null })).toBe(
    "Kopia - 1 snapshot in progress",
  );
  expect(trayToolTip({ uploading: 2, percent: 42 })).toBe(
    "Kopia - 2 snapshots in progress (42%)",
  );
});
