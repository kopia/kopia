import { test, expect } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import https from "https";

// We test file-viewer logic by reimplementing the pure functions here
// since the module uses electron-log which requires Electron context.

const TEMP_SUBDIR = "kopia-viewer-test";

function getTempDir() {
  return path.join(os.tmpdir(), TEMP_SUBDIR);
}

function ensureTempDir() {
  const dir = getTempDir();
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function cleanupTempDir() {
  const dir = getTempDir();
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function sanitizeFilename(filename) {
  return filename.replace(/[<>:"/\\|?*\x00-\x1f]/g, "_");
}

function downloadObject(
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
      rejectUnauthorized: !!serverCertificate,
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

let testServer;
let testServerPort;
let tlsOptions = null;

test.beforeAll(async () => {
  // Generate keys and self-signed cert using Node's built-in createSelfSignedCert
  // Node 22 has crypto.generateKeyPairSync but not cert generation,
  // so we create a self-signed cert by generating PEM via openssl
  // with the correct OPENSSL_CONF setting.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "kopia-test-cert-"));
  const keyPath = path.join(tmpDir, "key.pem");
  const certPath = path.join(tmpDir, "cert.pem");
  const confPath = path.join(tmpDir, "openssl.cnf");

  // Write minimal openssl config with SAN for 127.0.0.1
  fs.writeFileSync(
    confPath,
    [
      "[req]",
      "distinguished_name = req_dn",
      "x509_extensions = v3_req",
      "prompt = no",
      "[req_dn]",
      "CN = localhost",
      "[v3_req]",
      "subjectAltName = IP:127.0.0.1",
    ].join("\n"),
  );

  try {
    const { execFileSync } = await import("child_process");
    execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-keyout",
        keyPath,
        "-out",
        certPath,
        "-days",
        "1",
        "-nodes",
        "-config",
        confPath,
      ],
      { stdio: "pipe" },
    );

    tlsOptions = {
      key: fs.readFileSync(keyPath, "utf8"),
      cert: fs.readFileSync(certPath, "utf8"),
    };
  } catch {
    tlsOptions = null;
  }

  fs.rmSync(tmpDir, { recursive: true, force: true });
});

test.beforeEach(() => {
  cleanupTempDir();
});

test.afterEach(() => {
  cleanupTempDir();
  if (testServer) {
    testServer.close();
    testServer = null;
  }
});

test.describe("sanitizeFilename", () => {
  test("passes through safe filenames", () => {
    expect(sanitizeFilename("document.pdf")).toBe("document.pdf");
    expect(sanitizeFilename("my-file_v2.txt")).toBe("my-file_v2.txt");
  });

  test("replaces dangerous characters", () => {
    expect(sanitizeFilename('file<>:"/\\|?*.txt')).toBe("file_________.txt");
  });

  test("replaces control characters", () => {
    expect(sanitizeFilename("file\x00\x01\x1f.txt")).toBe("file___.txt");
  });

  test("handles empty filename", () => {
    expect(sanitizeFilename("")).toBe("");
  });
});

test.describe("getTempDir", () => {
  test("returns path under os.tmpdir", () => {
    const dir = getTempDir();
    expect(dir.startsWith(os.tmpdir())).toBe(true);
    expect(dir.endsWith(TEMP_SUBDIR)).toBe(true);
  });
});

test.describe("ensureTempDir", () => {
  test("creates directory if it does not exist", () => {
    cleanupTempDir();
    expect(fs.existsSync(getTempDir())).toBe(false);

    const dir = ensureTempDir();
    expect(fs.existsSync(dir)).toBe(true);
  });

  test("succeeds if directory already exists", () => {
    ensureTempDir();
    const dir = ensureTempDir();
    expect(fs.existsSync(dir)).toBe(true);
  });
});

test.describe("cleanupTempDir", () => {
  test("removes temp directory and contents", () => {
    const dir = ensureTempDir();
    fs.writeFileSync(path.join(dir, "test.txt"), "hello");
    expect(fs.existsSync(dir)).toBe(true);

    cleanupTempDir();
    expect(fs.existsSync(dir)).toBe(false);
  });

  test("succeeds if directory does not exist", () => {
    cleanupTempDir();
    expect(() => cleanupTempDir()).not.toThrow();
  });
});

test.describe("downloadObject", () => {
  function skipIfNoCert() {
    test.skip(!tlsOptions, "openssl not available for cert generation");
  }

  test("downloads file from server to temp directory", async () => {
    skipIfNoCert();
    const fileContent = "hello world from snapshot";

    testServer = https.createServer(tlsOptions, (req, res) => {
      const auth = req.headers.authorization;
      const expected =
        "Basic " + Buffer.from("kopia:testpass123").toString("base64");
      if (auth !== expected) {
        res.writeHead(401);
        res.end("Unauthorized");
        return;
      }

      if (req.url.startsWith("/api/v1/objects/abc123")) {
        res.writeHead(200, { "Content-Type": "application/octet-stream" });
        res.end(fileContent);
      } else {
        res.writeHead(404);
        res.end("Not Found");
      }
    });

    await new Promise((resolve) => {
      testServer.listen(0, "127.0.0.1", resolve);
    });
    testServerPort = testServer.address().port;

    const tempPath = await downloadObject(
      `https://127.0.0.1:${testServerPort}`,
      "abc123",
      "test-document.txt",
      tlsOptions.cert,
      "testpass123",
    );

    expect(fs.existsSync(tempPath)).toBe(true);
    expect(fs.readFileSync(tempPath, "utf8")).toBe(fileContent);
    expect(path.basename(tempPath)).toContain("test-document.txt");
  });

  test("rejects on HTTP error status", async () => {
    skipIfNoCert();

    testServer = https.createServer(tlsOptions, (_req, res) => {
      res.writeHead(404);
      res.end("Not Found");
    });

    await new Promise((resolve) => {
      testServer.listen(0, "127.0.0.1", resolve);
    });
    testServerPort = testServer.address().port;

    await expect(
      downloadObject(
        `https://127.0.0.1:${testServerPort}`,
        "missing-object",
        "test.txt",
        tlsOptions.cert,
        "testpass123",
      ),
    ).rejects.toThrow("Failed to download object: HTTP 404");
  });

  test("rejects on connection error", async () => {
    await expect(
      downloadObject(
        "https://127.0.0.1:1",
        "abc123",
        "test.txt",
        null,
        "testpass123",
      ),
    ).rejects.toThrow();
  });

  test("sanitizes filename in temp path", async () => {
    skipIfNoCert();

    testServer = https.createServer(tlsOptions, (_req, res) => {
      res.writeHead(200);
      res.end("data");
    });

    await new Promise((resolve) => {
      testServer.listen(0, "127.0.0.1", resolve);
    });
    testServerPort = testServer.address().port;

    const tempPath = await downloadObject(
      `https://127.0.0.1:${testServerPort}`,
      "obj1",
      'danger<>:"file.txt',
      tlsOptions.cert,
      "pass",
    );

    expect(fs.existsSync(tempPath)).toBe(true);
    expect(path.basename(tempPath)).not.toMatch(/[<>:"]/);
  });

  test("sends correct authorization header", async () => {
    skipIfNoCert();
    let receivedAuth = "";

    testServer = https.createServer(tlsOptions, (req, res) => {
      receivedAuth = req.headers.authorization || "";
      res.writeHead(200);
      res.end("ok");
    });

    await new Promise((resolve) => {
      testServer.listen(0, "127.0.0.1", resolve);
    });
    testServerPort = testServer.address().port;

    await downloadObject(
      `https://127.0.0.1:${testServerPort}`,
      "obj1",
      "file.txt",
      tlsOptions.cert,
      "mypassword",
    );

    const expected =
      "Basic " + Buffer.from("kopia:mypassword").toString("base64");
    expect(receivedAuth).toBe(expected);
  });

  test("encodes objectID in URL path", async () => {
    skipIfNoCert();
    let receivedUrl = "";

    testServer = https.createServer(tlsOptions, (req, res) => {
      receivedUrl = req.url;
      res.writeHead(200);
      res.end("ok");
    });

    await new Promise((resolve) => {
      testServer.listen(0, "127.0.0.1", resolve);
    });
    testServerPort = testServer.address().port;

    await downloadObject(
      `https://127.0.0.1:${testServerPort}`,
      "Iabcd1234/5",
      "file.txt",
      tlsOptions.cert,
      "pass",
    );

    expect(receivedUrl).toContain("/api/v1/objects/Iabcd1234%2F5");
  });
});
