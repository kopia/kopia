import {
  app,
  BrowserWindow,
  Notification,
  screen,
  Menu,
  Tray,
  ipcMain,
  dialog,
  shell,
} from "electron";
import pkg from "electron-updater";
const autoUpdater = pkg.autoUpdater;
import { iconsPath, publicPath, selectByOS } from "./utils.js";
import {
  toggleLaunchAtStartup,
  willLaunchAtStartup,
  refreshWillLaunchAtStartup,
} from "./auto-launch.js";
import {
  setNotificationLevel,
  getNotificationLevel,
  LevelDisabled,
  LevelWarningsAndErrors,
} from "./notifications.js";
import { serverForRepo } from "./server.js";
import {
  applySnapshotReport,
  parseSnapshotReport,
  pruneFailures,
  shortenPath,
  sourceStatusLabel,
  trayIconState,
  traySummary,
  trayTitle,
  trayToolTip,
  withFailures,
} from "./tray-status.js";
import {
  loadConfigs,
  allConfigs,
  deleteConfigIfDisconnected,
  addNewConfig,
  configDir,
  isFirstRun,
  isPortableConfig,
} from "./config.js";

import Store from "electron-store";
import log from "electron-log";
import path from "path";
import crypto from "crypto";
import os from "os";

// Store to save parameters
const store = new Store();

app.name = "KopiaUI";

let tray = null;
let repositoryWindows = {};
let repoIDForWebContents = {};

if (process.env.KOPIA_CUSTOM_APPDATA) {
  app.setPath("appData", process.env.KOPIA_CUSTOM_APPDATA);
}

if (isPortableConfig()) {
  // in portable mode, write cache under 'repositories'
  app.setPath("userData", path.join(configDir(), "cache"));
}

/**
 * Stores the ids of the currently connected displays.
 * The ids are sorted to generate a hash that specifies the current display configuration
 * @returns A hash of the configuration
 */
function getDisplayConfiguration() {
  // Stores the IDs all all currently connected displays
  let config = [];
  let sha256 = crypto.createHash("sha256");
  // Get all displays
  let displays = screen.getAllDisplays();
  let isFactorEqual = false;
  // Stores the previous factor - initialized with the primary scaling factor
  let prevFactor = screen.getPrimaryDisplay().scaleFactor;
  //Workaround until https://github.com/electron/electron/issues/10862 is fixed
  for (let dsp in displays) {
    // Add the id to the config
    config.push(displays[dsp].id);
    isFactorEqual = prevFactor === displays[dsp].scaleFactor;
    // Update the previous factors
    prevFactor = displays[dsp].scaleFactor;
  }
  // Sort IDs to prevent different hashes through permutation
  config.sort();
  sha256.update(config.toString());
  return { hash: sha256.digest("hex"), factorsEqual: isFactorEqual };
}

/**
 * Creates a repository window with given options and parameters
 * @param {*} repositoryID
 * The id for that specific repository used as a reference for that window
 */
function showRepoWindow(repositoryID) {
  let primaryScreenBounds = screen.getPrimaryDisplay().bounds;
  if (repositoryWindows[repositoryID]) {
    repositoryWindows[repositoryID].focus();
    return;
  }

  let windowOptions = {
    title: "KopiaUI is Loading...",
    // default width
    width: 1000,
    // default height
    height: 700,
    // default x location
    x: (primaryScreenBounds.width - 1000) / 2,
    // default y location
    y: (primaryScreenBounds.height - 700) / 2,
    autoHideMenuBar: true,
    resizable: true,
    show: false,
    webPreferences: {
      preload: path.join(publicPath(), "preload.js"),
    },
  };

  // The bounds of the windows
  let configuration = getDisplayConfiguration();
  let winBounds = store.get(configuration.hash);
  let maximized = store.get("maximized");

  if (configuration.factorsEqual) {
    Object.assign(windowOptions, winBounds);
  }

  // Create the browser window
  let repositoryWindow = new BrowserWindow(windowOptions);
  // If the window was maximized, maximize it
  if (maximized) {
    repositoryWindow.maximize();
  }
  const webContentsID = repositoryWindow.webContents.id;
  repositoryWindows[repositoryID] = repositoryWindow;
  repoIDForWebContents[webContentsID] = repositoryID;

  // Failed to load the content, retry
  repositoryWindow.webContents.on("did-fail-load", () => {
    log.error("failed to load content");

    // schedule another attempt in 0.5s
    if (repositoryWindows[repositoryID]) {
      setTimeout(() => {
        log.info("reloading");
        repositoryWindows[repositoryID].loadURL(
          serverForRepo(repositoryID).getServerAddress() +
            "/?ts=" +
            new Date().valueOf(),
        );
      }, 500);
    }
  });

  repositoryWindow.loadURL(
    serverForRepo(repositoryID).getServerAddress() +
      "/?ts=" +
      new Date().valueOf(),
  );
  updateDockIcon();

  /**
   * Store the window size, height and position on close
   */
  repositoryWindow.on("close", function () {
    store.set(getDisplayConfiguration().hash, repositoryWindow.getBounds());
    store.set("maximized", repositoryWindow.isMaximized());
  });

  /**
   * Show the window once the content is ready
   */
  repositoryWindow.once("ready-to-show", function () {
    repositoryWindow.show();
  });

  /**
   * Delete references to the repository window
   */
  repositoryWindow.on("closed", function () {
    // Delete the reference to the window
    repositoryWindow = null;
    delete repositoryWindows[repositoryID];
    delete repoIDForWebContents[webContentsID];

    const s = serverForRepo(repositoryID);
    if (deleteConfigIfDisconnected(repositoryID)) {
      s.stopServer();
    }
    updateDockIcon();
  });
}

// Check if another instance of kopia is running
if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on("second-instance", (_event, _commandLine, _workingDirectory) => {
    // Someone tried to run a second instance, we should focus our window.
    for (let repositoryID in repositoryWindows) {
      let rw = repositoryWindows[repositoryID];
      if (rw.isMinimized()) {
        rw.restore();
      }
      rw.focus();
    }
  });
}

app.on("will-quit", function () {
  allConfigs().forEach((repositoryID) =>
    serverForRepo(repositoryID).stopServer(),
  );
});

app.on("login", (event, webContents, _request, _authInfo, callback) => {
  const repositoryID = repoIDForWebContents[webContents.id];

  // intercept password prompts and automatically enter password that the server has printed for us.
  const password = serverForRepo(repositoryID).getServerPassword();
  if (password) {
    event.preventDefault();
    log.info("automatically logging in...");
    callback("kopia", password);
  }
});

app.on(
  "certificate-error",
  (event, webContents, _url, _error, certificate, callback) => {
    const repositoryID = repoIDForWebContents[webContents.id];
    // intercept certificate errors and automatically trust the certificate the server has printed for us.
    const expected =
      "sha256/" +
      Buffer.from(
        serverForRepo(repositoryID).getServerCertSHA256(),
        "hex",
      ).toString("base64");
    if (certificate.fingerprint === expected) {
      log.debug("accepting server certificate.");

      // On certificate error we disable default behaviour (stop loading the page)
      // and we then say "it is all fine - true" to the callback
      event.preventDefault();
      callback(true);
      return;
    }

    log.warn("certificate error:", certificate.fingerprint, expected);
  },
);

/**
 * Ignore to let the application run, when all windows are closed
 */
app.on("window-all-closed", function () {});

ipcMain.handle("select-dir", async (_event, _arg) => {
  const result = await dialog.showOpenDialog({
    properties: ["openDirectory"],
  });

  if (result.filePaths) {
    return result.filePaths[0];
  } else {
    return null;
  }
});

ipcMain.handle("browse-dir", async (_event, path) => {
  shell.openPath(path);
});

ipcMain.on("server-status-updated", updateTrayContextMenu);
ipcMain.on("launch-at-startup-updated", updateTrayContextMenu);
ipcMain.on("notification-config-updated", updateTrayContextMenu);

let updateAvailableInfo = null;
let updateDownloadStatusInfo = "";
let updateFailed = false;
let checkForUpdatesTriggeredFromUI = false;

// set this environment variable when developing
// to allow offering downgrade to the latest released version.
autoUpdater.allowDowngrade = process.env["KOPIA_UI_ALLOW_DOWNGRADE"] == "1";

// we will be manually triggering download and quit&install.
autoUpdater.autoDownload = false;
autoUpdater.autoInstallOnAppQuit = false;

let lastNotifiedVersion = "";

autoUpdater.on("update-available", (a) => {
  log.info("update available " + a.version);

  updateAvailableInfo = a;
  updateDownloadStatusInfo = "";
  updateTrayContextMenu();

  // do not notify more than once for a particular version.
  if (checkForUpdatesTriggeredFromUI) {
    dialog
      .showMessageBox({
        buttons: ["Yes", "No"],
        message:
          "An updated KopiaUI v" +
          a.version +
          " is available.\n\nDo you want to install it now?",
      })
      .then((r) => {
        if (r.response == 0) {
          installUpdate();
        }
      });
    checkForUpdatesTriggeredFromUI = false;
  }

  if (lastNotifiedVersion != a.version) {
    lastNotifiedVersion = a.version;

    const notification = new Notification({
      title: "New version of KopiaUI",
      body:
        "Version v" +
        a.version +
        " is available.\n\nClick here to download and install it.",
    });

    notification.on("click", () => installUpdate());
    notification.show();
  }
});

autoUpdater.on("update-not-available", () => {
  updateAvailableInfo = null;
  updateDownloadStatusInfo = "";
  updateFailed = false;
  updateTrayContextMenu();
  if (checkForUpdatesTriggeredFromUI) {
    dialog.showMessageBox({
      buttons: ["OK"],
      message: "No updates available.",
    });
    checkForUpdatesTriggeredFromUI = false;
  }
});

autoUpdater.on("download-progress", (progress) => {
  if (updateAvailableInfo) {
    updateDownloadStatusInfo =
      "Downloading Update: v" +
      updateAvailableInfo.version +
      " (" +
      Math.round(progress.percent * 10) / 10.0 +
      "%) ...";
    updateTrayContextMenu();
  }
});

autoUpdater.on("update-downloaded", (_info) => {
  updateDownloadStatusInfo =
    "Installing Update: v" + updateAvailableInfo.version + " ...";
  updateTrayContextMenu();

  setTimeout(() => {
    try {
      autoUpdater.quitAndInstall();
    } catch (e) {
      log.info("update error", e);
    }

    updateDownloadStatusInfo = null;
    updateFailed = true;
    updateTrayContextMenu();
  }, 500);
});

autoUpdater.on("error", (a) => {
  updateAvailableInfo = null;
  updateDownloadStatusInfo = "Error checking for updates.";
  log.info("error checking for updates", a);
  updateTrayContextMenu();
  if (checkForUpdatesTriggeredFromUI) {
    dialog.showErrorBox(
      "Error checking for updates.",
      "There was an error checking for updates, try again later.",
    );
    checkForUpdatesTriggeredFromUI = false;
  }
});

function checkForUpdates() {
  updateDownloadStatusInfo = "Checking for update...";
  updateAvailableInfo = null;
  updateTrayContextMenu();

  autoUpdater.checkForUpdates();
}

function checkForUpdatesNow() {
  checkForUpdatesTriggeredFromUI = true;
  checkForUpdates();
}

function installUpdate() {
  updateDownloadStatusInfo = "Downloading and installing update...";
  autoUpdater.downloadUpdate();
}

function viewReleaseNotes() {
  const ver = updateAvailableInfo.version + "";
  if (ver.match(/^\d{8}\./)) {
    // kopia-test builds are named yyyymmdd.0.hhmmss
    shell.openExternal(
      "https://github.com/kopia/kopia-test-builds/releases/v" + ver,
    );
  } else {
    shell.openExternal("https://github.com/kopia/kopia/releases/v" + ver);
  }
}

function isOutsideOfApplicationsFolderOnMac() {
  if (!app.isPackaged || isPortableConfig()) {
    return false;
  }

  // this method is only available on Mac.
  if (!app.isInApplicationsFolder) {
    return false;
  }

  return !app.isInApplicationsFolder();
}

function maybeMoveToApplicationsFolder() {
  if (process.env["KOPIA_UI_TESTING"]) {
    return;
  }

  dialog
    .showMessageBox({
      buttons: ["Yes", "No"],
      message:
        "For best experience, Kopia needs to be installed in Applications folder.\n\nDo you want to move it now?",
    })
    .then((r) => {
      if (r.response == 0) {
        app.moveToApplicationsFolder();
      } else {
        checkForUpdates();
      }
    })
    .catch((e) => {
      log.info(e);
    });
}

function updateDockIcon() {
  if (process.platform === "darwin") {
    let any = false;
    for (const _k in repositoryWindows) {
      any = true;
    }
    if (any) {
      app.dock.show();
    } else {
      app.dock.hide();
    }
  }
}

/**
 * Show all repository windows at once
 */
function showAllRepoWindows() {
  allConfigs().forEach(showRepoWindow);
}

function safeTrayHandler(ev, h) {
  tray.on(ev, () => {
    try {
      h();
    } catch (e) {}
  });
}

app.on("ready", () => {
  loadConfigs();

  if (isPortableConfig()) {
    const logDir = path.join(configDir(), "logs");

    log.transports.file.resolvePath = (variables) =>
      path.join(logDir, variables.fileName);
  }

  log.transports.console.level = "warn";
  log.transports.file.level = "debug";
  autoUpdater.logger = log;

  // re-check for updates every 24 hours
  setInterval(checkForUpdates, 86400000);

  tray = new Tray(trayIconPath(trayIcon));

  // hooks exposed to tests
  if (process.env["KOPIA_UI_TESTING"]) {
    app.testHooks = {
      tray: tray,
      showRepoWindow: showRepoWindow,
      allConfigs: allConfigs,
    };
  }

  safeTrayHandler("click", () => tray.popUpContextMenu());
  safeTrayHandler("right-click", () => tray.popUpContextMenu());
  safeTrayHandler("double-click", () => showAllRepoWindows());

  updateTrayContextMenu();
  refreshWillLaunchAtStartup();
  updateDockIcon();

  allConfigs().forEach((repoID) => serverForRepo(repoID).actuateServer());

  if (isFirstRun()) {
    // open all repo windows on first run.
    showAllRepoWindows();

    // on Windows, also show the notification.
    if (process.platform === "win32") {
      tray.displayBalloon({
        title: "Kopia is running in the background",
        content: "Click on the system tray icon to open the menu",
      });
    }
  }

  if (isOutsideOfApplicationsFolderOnMac()) {
    setTimeout(maybeMoveToApplicationsFolder, 1000);
  } else {
    checkForUpdates();
  }
});

function showRepoNotification(e) {
  const nl = getNotificationLevel();
  if (nl === LevelDisabled) {
    // notifications disabled
    return;
  }

  const severity = e.notification.severity;
  if (severity < 10 && nl === LevelWarningsAndErrors) {
    log.info(
      "showRepoNotification",
      "skipping notification",
      e.notification.subject,
    );
    return;
  }

  let urgency = "normal";

  if (severity < 0) {
    urgency = "low";
  } else if (severity >= 10) {
    // warnings and errors
    urgency = "critical";
  } else {
    urgency = "normal";
  }

  const notification = new Notification({
    title: e.notification.subject,
    body: e.notification.body,
    urgency: urgency,
  });

  notification.on("click", () => showRepoWindow(e.repositoryID));
  notification.show();
}

ipcMain.addListener("config-list-updated-event", () => updateTrayContextMenu());
ipcMain.addListener("status-updated-event", () => updateTrayContextMenu());
ipcMain.addListener("repo-notification-event", showRepoNotification);
ipcMain.addListener("repo-notification-event", recordSnapshotReport);

function addAnotherRepository() {
  const repoID = addNewConfig();
  serverForRepo(repoID).actuateServer();
  showRepoWindow(repoID);
}

const trayIcons = {
  idle: {
    mac: "kopiaTrayTemplate.png",
    win: "kopia-tray.ico",
    linux: "kopia-tray.png",
  },
  busy: {
    mac: "kopiaTrayBusyTemplate.png",
    win: "kopia-tray-busy.ico",
    linux: "kopia-tray-busy.png",
  },
  error: {
    // not a template image so it stays red in the macOS menu bar.
    mac: "kopiaTrayError.png",
    win: "kopia-tray-error.ico",
    linux: "kopia-tray-error.png",
  },
};

let trayIcon = "idle";
let trayMenuSignature = "";

function trayIconPath(state) {
  return path.join(iconsPath(), selectByOS(trayIcons[state]));
}

// repoID -> source path -> "fatal" | "warnings", persisted so failures remain
// visible after a restart until that source's next successful snapshot.
let sourceFailures = store.get("sourceFailures", {});

function setSourceFailures(updated) {
  if (JSON.stringify(updated) === JSON.stringify(sourceFailures)) {
    return false;
  }

  sourceFailures = updated;
  store.set("sourceFailures", sourceFailures);
  return true;
}

// repoID -> when its last snapshot report arrived. Source lists fetched before
// then may not include a new source yet, so they aren't used for pruning.
const lastReportAt = {};

function recordSnapshotReport(e) {
  const results = parseSnapshotReport(e.notification?.body);
  if (!results.length) {
    if (/snapshot/i.test(e.notification?.subject)) {
      log.warn(
        "snapshot report has no Path/Status lines, customised template?",
        e.notification.subject,
      );
    }
    return;
  }

  lastReportAt[e.repoID] = Date.now();
  const updated = {
    ...sourceFailures,
    [e.repoID]: applySnapshotReport(sourceFailures[e.repoID] || {}, results),
  };
  if (setSourceFailures(updated)) {
    updateTrayContextMenu();
  }
}

// "~" is not a familiar shorthand for the home directory on Windows.
const homeDirAlias = process.platform === "win32" ? "" : os.homedir();

// Windows and Linux treat "&" in menu labels as an accelerator prefix.
function escapeMenuLabel(label) {
  return process.platform === "darwin" ? label : label.replace(/&/g, "&&");
}

function updateTrayContextMenu() {
  if (!tray) {
    return;
  }

  let defaultReposTemplates = [];
  let additionalReposTemplates = [];
  let allSourceStatuses = [];

  const knownSources = {};
  allConfigs().forEach((repoID) => {
    const s = serverForRepo(repoID);
    const sd = s.getServerStatusDetails();
    if (!sd.startingUp && !sd.connected && !sd.initTaskID) {
      // nothing is configured in this slot (disconnected), so failures must
      // not carry over to the next repository connected here. An unreachable
      // repository keeps retrying with an initTaskID and keeps its failures.
      knownSources[repoID] = [];
    } else if (s.getSourcesFetchedAt() > (lastReportAt[repoID] || 0)) {
      knownSources[repoID] = s.getSourceStatuses().map((st) => st.path);
    } else {
      knownSources[repoID] = null;
    }
  });
  setSourceFailures(pruneFailures(sourceFailures, knownSources));

  allConfigs().forEach((repoID) => {
    const sd = serverForRepo(repoID).getServerStatusDetails();
    const sourceStatuses = withFailures(
      serverForRepo(repoID).getSourceStatuses(),
      sourceFailures[repoID] || {},
    );
    allSourceStatuses = allSourceStatuses.concat(sourceStatuses);
    let desc = "";

    if (sd.startingUp) {
      desc = "<starting up>";
    } else if (!sd.connected) {
      if (sd.initTaskID) {
        desc = "<initializing>";
      } else {
        desc = "<not connected>";
      }
    } else {
      desc = sd.description;
    }

    // put primary repository first.
    const collection =
      repoID === "repository"
        ? defaultReposTemplates
        : additionalReposTemplates;

    // each repository is followed by its snapshot sources, so sort as a group.
    collection.push([
      {
        label: desc,
        click: () => showRepoWindow(repoID),
        toolTip: desc + " (" + repoID + ")",
      },
      ...sourceStatuses.map((s) => ({
        label: escapeMenuLabel(
          "    " +
            shortenPath(s.path, homeDirAlias) +
            (s.otherUser ? " (" + s.otherUser + ")" : "") +
            " - " +
            sourceStatusLabel(s),
        ),
        click: () => showRepoWindow(repoID),
        toolTip: s.path,
      })),
    ]);
  });

  if (additionalReposTemplates.length > 0) {
    additionalReposTemplates.sort((a, b) =>
      a[0].label.localeCompare(b[0].label),
    );
  }

  const summary = traySummary(allSourceStatuses);
  const icon = trayIconState(summary);
  if (icon !== trayIcon) {
    trayIcon = icon;
    tray.setImage(trayIconPath(icon));
  }
  tray.setToolTip(trayToolTip(summary));
  if (process.platform === "darwin") {
    tray.setTitle(trayTitle(summary), { fontType: "monospacedDigit" });
  }

  let autoUpdateMenuItems = [];

  if (updateDownloadStatusInfo) {
    autoUpdateMenuItems.push({
      label: updateDownloadStatusInfo,
      enabled: false,
    });
  } else if (updateAvailableInfo) {
    if (updateFailed) {
      autoUpdateMenuItems.push({
        label:
          "Update Failed, click to manually download and install v" +
          updateAvailableInfo.version,
        click: viewReleaseNotes,
      });
    } else {
      autoUpdateMenuItems.push({
        label: "Update Available: v" + updateAvailableInfo.version,
        click: viewReleaseNotes,
      });
      autoUpdateMenuItems.push({
        label: "Download And Install...",
        click: installUpdate,
      });
    }
  } else {
    autoUpdateMenuItems.push({
      label: "KopiaUI is up-to-date: " + app.getVersion(),
      enabled: false,
    });
  }

  const nl = getNotificationLevel();

  let template = defaultReposTemplates
    .concat(additionalReposTemplates)
    .flat()
    .concat([
      { type: "separator" },
      {
        label: "Connect To Another Repository...",
        click: addAnotherRepository,
      },
      { type: "separator" },
      { label: "Check For Updates Now", click: checkForUpdatesNow },
    ])
    .concat(autoUpdateMenuItems)
    .concat([
      { type: "separator" },
      {
        label: "Launch At Startup",
        type: "checkbox",
        click: toggleLaunchAtStartup,
        checked: willLaunchAtStartup(),
      },
      {
        label: "Notifications",
        type: "submenu",
        submenu: [
          {
            label: "Enabled",
            type: "radio",
            click: () => setNotificationLevel(2),
            checked: nl === 2,
          },
          {
            label: "Warnings And Errors",
            type: "radio",
            click: () => setNotificationLevel(1),
            checked: nl === 1,
          },
          {
            label: "Disabled",
            type: "radio",
            click: () => setNotificationLevel(0),
            checked: nl === 0,
          },
        ],
      },
      { label: "Quit", role: "quit" },
    ]);

  // progress polls update the title and tooltip every few seconds, the menu
  // is only rebuilt when its contents change.
  const menuSignature = JSON.stringify(template);
  if (menuSignature !== trayMenuSignature) {
    trayMenuSignature = menuSignature;
    tray.setContextMenu(Menu.buildFromTemplate(template));
  }
}
