import fs from "fs";
import { fileURLToPath } from "url";

const fallbackLocale = "en";
const localesDirectory = fileURLToPath(new URL("./locales/", import.meta.url));

function loadCatalogs() {
  const result = {};

  for (const fileName of fs.readdirSync(localesDirectory)) {
    if (!fileName.endsWith(".json")) {
      continue;
    }

    const locale = fileName.slice(0, -5);
    result[locale] = JSON.parse(
      fs.readFileSync(
        new URL(`./locales/${fileName}`, import.meta.url),
        "utf8",
      ),
    );
  }

  return result;
}

export const catalogs = loadCatalogs();
let activeLocale = fallbackLocale;

export function resolveLocale(language) {
  const normalized = String(language || "")
    .replace("_", "-")
    .toLowerCase();

  const exactMatch = Object.keys(catalogs).find(
    (locale) => locale.toLowerCase() === normalized,
  );
  if (exactMatch) {
    return exactMatch;
  }

  const languageCode = normalized.split("-")[0];
  const languageMatch = Object.keys(catalogs).find(
    (locale) => locale.toLowerCase().split("-")[0] === languageCode,
  );

  return languageMatch || fallbackLocale;
}

export function setLocale(language) {
  activeLocale = resolveLocale(language);
  return activeLocale;
}

export function getLocale() {
  return activeLocale;
}

export function t(key, replacements = {}) {
  let value =
    catalogs[activeLocale]?.[key] ?? catalogs[fallbackLocale]?.[key] ?? key;

  for (const [name, replacement] of Object.entries(replacements)) {
    value = value.split(`{{${name}}}`).join(String(replacement));
  }

  return value;
}
