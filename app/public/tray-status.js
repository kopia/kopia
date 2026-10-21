// Helpers that turn server source status into tray labels.
// Kept free of Electron imports so they can be unit tested.

const statusLabels = {
  IDLE: "Idle",
  PENDING: "Pending",
  UPLOADING: "Uploading",
  PAUSED: "Paused",
  FAILED: "Failed",
};

/**
 * Returns whole-number percent complete for upload counters, or null when
 * the estimate is unknown or has been exceeded (same rule as the HTML UI).
 */
export function uploadPercent(doneBytes, estimatedBytes) {
  if (!estimatedBytes) {
    return null;
  }

  const percent = Math.floor((doneBytes * 100) / estimatedBytes);
  if (percent > 100) {
    return null;
  }

  return percent;
}

/**
 * Reduces a /api/v1/control/sources response to the sources on the local
 * host, keeping only the fields the tray displays. The server snapshots every
 * user's sources on its host, so other users' sources are kept and named.
 */
export function localSourceStatuses(resp) {
  return (resp?.sources || [])
    .filter((s) => s.source?.host === resp.localHost)
    .map((s) => {
      const u = s.upload;
      const doneBytes = u ? (u.hashedBytes || 0) + (u.cachedBytes || 0) : 0;
      const estimatedBytes = u?.estimatedBytes || 0;

      return {
        path: s.source.path,
        otherUser:
          s.source.userName === resp.localUsername ? null : s.source.userName,
        status: s.status,
        doneBytes,
        estimatedBytes,
        percent: uploadPercent(doneBytes, estimatedBytes),
      };
    });
}

// labels for an idle source whose last snapshot report was not successful.
const failureLabels = {
  fatal: "Failed",
  warnings: "Errors",
};

export function sourceStatusLabel(s) {
  if (s.status === "UPLOADING" && s.percent !== null) {
    return s.percent + "%";
  }

  if (s.status === "IDLE" && s.failure) {
    return failureLabels[s.failure];
  }

  return statusLabels[s.status] || s.status;
}

/**
 * Extracts per-source results from the plain text body of a snapshot report
 * notification. The server only reports failures this way, so this relies on
 * the default "Path:" and "Status:" lines of the snapshot-report template.
 * Repositories with a customised template, or KopiaUI connected to a
 * repository server (which receives the notifications instead), report none.
 */
export function parseSnapshotReport(body) {
  const results = [];
  const re = /^Path: (.+)\n\s*\n\s+Status:\s+(\w+)/gm;

  for (const m of (body || "").matchAll(re)) {
    results.push({ path: m[1], status: m[2] });
  }

  return results;
}

/**
 * Returns failures (path to "fatal" or "warnings") updated with snapshot
 * report results. Only a successful snapshot of the same path clears it;
 * incomplete (cancelled) snapshots leave it unchanged.
 */
export function applySnapshotReport(failures, results) {
  const updated = { ...failures };

  for (const r of results) {
    if (r.status === "success") {
      delete updated[r.path];
    } else if (failureLabels[r.status]) {
      updated[r.path] = r.status;
    }
  }

  return updated;
}

/**
 * Adds each source's last snapshot failure. Reports identify sources by path
 * only, so a path snapshotted by several users is left without one rather
 * than showing another user's result.
 */
export function withFailures(statuses, failures) {
  const counts = {};
  for (const s of statuses) {
    counts[s.path] = (counts[s.path] || 0) + 1;
  }

  return statuses.map((s) => ({
    ...s,
    failure: counts[s.path] === 1 ? failures[s.path] : undefined,
  }));
}

/**
 * Drops failures for repositories and sources that no longer exist.
 * knownSources maps repoID to its current source paths, or null when the
 * list is not known yet (starting up, not connected), which keeps them.
 */
export function pruneFailures(failures, knownSources) {
  const pruned = {};

  for (const [repoID, paths] of Object.entries(knownSources)) {
    const f = failures[repoID];
    if (!f) {
      continue;
    }

    if (paths === null) {
      pruned[repoID] = f;
      continue;
    }

    const kept = Object.fromEntries(
      Object.entries(f).filter(([p]) => paths.includes(p)),
    );
    if (Object.keys(kept).length) {
      pruned[repoID] = kept;
    }
  }

  return pruned;
}

/**
 * Replaces the home directory prefix with "~" and trims the middle of long
 * paths so tray menus stay narrow.
 */
export function shortenPath(p, homeDir, maxLength = 40) {
  if (homeDir && (p === homeDir || p.startsWith(homeDir + "/"))) {
    p = "~" + p.substring(homeDir.length);
  }

  if (p.length <= maxLength) {
    return p;
  }

  const keep = maxLength - 3;
  const head = Math.ceil(keep / 2);
  return p.substring(0, head) + "..." + p.substring(p.length - (keep - head));
}

/**
 * Summarises uploads and failures across all sources. Percent is weighted by
 * estimated size and is null when no running upload has an estimate yet.
 */
export function traySummary(statuses) {
  const uploading = statuses.filter((s) => s.status === "UPLOADING");
  const estimated = uploading.filter((s) => s.percent !== null);

  const doneBytes = estimated.reduce((sum, s) => sum + s.doneBytes, 0);
  const estimatedBytes = estimated.reduce(
    (sum, s) => sum + s.estimatedBytes,
    0,
  );

  return {
    uploading: uploading.length,
    percent: uploadPercent(doneBytes, estimatedBytes),
    failed: statuses.filter((s) => s.failure).length,
  };
}

// icon variant: an upload in progress takes priority over earlier failures.
export function trayIconState(summary) {
  if (summary.uploading) {
    return "busy";
  }

  return summary.failed ? "error" : "idle";
}

// the busy icon already shows an upload is running, the title adds the percent once known.
export function trayTitle(summary) {
  if (!summary.uploading || summary.percent === null) {
    return "";
  }

  return summary.percent + "%";
}

export function trayToolTip(summary) {
  const parts = [];

  if (summary.uploading) {
    let p =
      summary.uploading +
      (summary.uploading === 1 ? " snapshot" : " snapshots") +
      " in progress";

    if (summary.percent !== null) {
      p += " (" + summary.percent + "%)";
    }

    parts.push(p);
  }

  if (summary.failed) {
    parts.push(
      summary.failed +
        (summary.failed === 1 ? " source" : " sources") +
        " with errors",
    );
  }

  return parts.length ? "Kopia - " + parts.join(", ") : "Kopia";
}
