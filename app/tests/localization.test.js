import assert from "node:assert/strict";
import test from "node:test";
import {
  catalogs,
  getLocale,
  resolveLocale,
  setLocale,
  t,
} from "../public/localization.js";

test("all locale catalogs contain the same keys as English", () => {
  const englishKeys = Object.keys(catalogs.en).sort();

  for (const [locale, catalog] of Object.entries(catalogs)) {
    assert.deepEqual(Object.keys(catalog).sort(), englishKeys, locale);
  }
});

test("resolves regional and language-only locale names", () => {
  assert.equal(resolveLocale("pt-BR"), "pt-BR");
  assert.equal(resolveLocale("pt_BR"), "pt-BR");
  assert.equal(resolveLocale("pt"), "pt-BR");
  assert.equal(resolveLocale("unsupported"), "en");
});

test("translates messages, replacements and fallbacks", () => {
  setLocale("pt-BR");
  assert.equal(getLocale(), "pt-BR");
  assert.equal(t("tray.quit"), "Sair");
  assert.equal(
    t("updates.upToDate", { version: "0.23.1" }),
    "KopiaUI está atualizado: 0.23.1",
  );
  assert.equal(t("missing.key"), "missing.key");

  setLocale("en");
});
