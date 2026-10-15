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
 * Reduces a /api/v1/control/sources response to the sources owned by the
 * local user and host, keeping only the fields the tray displays.
 */
export function localSourceStatuses(resp) {
  return (resp?.sources || [])
    .filter(
      (s) =>
        s.source?.host === resp.localHost &&
        s.source?.userName === resp.localUsername,
    )
    .map((s) => {
      const u = s.upload;
      const doneBytes = u ? (u.hashedBytes || 0) + (u.cachedBytes || 0) : 0;
      const estimatedBytes = u?.estimatedBytes || 0;

      return {
        path: s.source.path,
        status: s.status,
        doneBytes,
        estimatedBytes,
        percent: uploadPercent(doneBytes, estimatedBytes),
      };
    });
}

export function sourceStatusLabel(s) {
  if (s.status === "UPLOADING" && s.percent !== null) {
    return s.percent + "%";
  }

  return statusLabels[s.status] || s.status;
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
 * Summarises uploads across all sources. Percent is weighted by estimated
 * size and is null when no running upload has an estimate yet.
 */
export function overallProgress(statuses) {
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
  };
}

export function trayTitle(progress) {
  if (!progress.uploading) {
    return "";
  }

  return progress.percent === null ? "..." : progress.percent + "%";
}

export function trayToolTip(progress) {
  if (!progress.uploading) {
    return "Kopia";
  }

  let tip =
    "Kopia - " +
    progress.uploading +
    (progress.uploading === 1 ? " snapshot" : " snapshots") +
    " in progress";

  if (progress.percent !== null) {
    tip += " (" + progress.percent + "%)";
  }

  return tip;
}
