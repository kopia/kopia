import { test, expect } from "@playwright/test";

import { lineBuffer } from "../public/line-buffer.js";

test("lineBuffer joins a line split across chunks", () => {
  const lines = lineBuffer();

  expect(
    lines('SERVER ADDRESS: https://127.0.0.1:1234\nNOTIFICATION: {"sub'),
  ).toStrictEqual(["SERVER ADDRESS: https://127.0.0.1:1234"]);
  expect(lines('ject":"x"')).toStrictEqual([]);
  expect(lines("}\nnext")).toStrictEqual(['NOTIFICATION: {"subject":"x"}']);
  expect(lines("\n")).toStrictEqual(["next"]);
});

test("lineBuffer returns several lines from one chunk", () => {
  const lines = lineBuffer();

  expect(lines("a\nb\n\nc\n")).toStrictEqual(["a", "b", "", "c"]);
});
