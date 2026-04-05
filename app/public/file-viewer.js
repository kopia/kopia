import https from "https";
import fs from "fs";
import os from "os";
import path from "path";
import log from "electron-log";

const TEMP_SUBDIR = "kopia-viewer";

export function getTempDir() {
  return path.join(os.tmpdir(), TEMP_SUBDIR);
}

export function ensureTempDir() {
  const dir = getTempDir();
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

export function cleanupTempDir() {
  const dir = getTempDir();
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

export function sanitizeFilename(filename) {
  return filename.replace(/[<>:"/\\|?*\x00-\x1f]/g, "_");
}

export function downloadObject(
  serverAddress,
  objectID,
  filename,
  serverCertificate,
  password,
) {
  return new Promise((resolve, reject) => {
    const url = new URL(
      `/api/v1/objects/${encodeURIComponent(objectID)}`,
      serverAddress,
    );
    url.searchParams.set("fname", filename);

    const tempDir = ensureTempDir();
    const safeName = sanitizeFilename(filename);
    const tempPath = path.join(tempDir, `${Date.now()}-${safeName}`);

    const options = {
      ca: serverCertificate ? [serverCertificate] : undefined,
      headers: {
        Authorization:
          "Basic " + Buffer.from("kopia:" + password).toString("base64"),
      },
    };

    https
      .get(url, options, (resp) => {
        if (resp.statusCode !== 200) {
          resp.resume();
          reject(
            new Error(`Failed to download object: HTTP ${resp.statusCode}`),
          );
          return;
        }

        const file = fs.createWriteStream(tempPath);
        resp.pipe(file);
        file.on("finish", () => {
          file.close(() => resolve(tempPath));
        });
        file.on("error", (err) => {
          fs.unlink(tempPath, () => {});
          reject(err);
        });
      })
      .on("error", (err) => {
        reject(err);
      });
  });
}
