package htmluie2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

//nolint:thelper
func runInBrowser(t *testing.T, run func(ctx context.Context, sp *testutil.ServerParameters, tc *TestContext)) {
	if os.Getenv("HTMLUI_E2E_TEST") == "" {
		t.Skip()
	}

	tc := TestContext{
		t:                t,
		downloadFinished: make(chan string, 10),
	}

	// setup directory where we will be capturing screenshots
	sd, err := filepath.Abs("../../.screenshots/htmlui-e2e-test/" + t.Name())
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(sd))
	require.NoError(t, os.MkdirAll(sd, 0o755))
	tc.screenshotsDir = sd

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	var sp testutil.ServerParameters

	_, kill := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--random-password",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048",
		"--insecure",
		"--without-password",
		"--override-hostname=the-hostname",
		"--override-username=the-username",
		// "--html="+os.Getenv("HTMLUI_BUILD_DIR"),
	)
	defer kill()

	t.Logf("detected server parameters %#v", sp)

	ctx, cancelTimeout := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelTimeout()

	maybeHeadless := chromedp.Headless
	if os.Getenv("CI") == "" {
		maybeHeadless = func(a *chromedp.ExecAllocator) {}
	}

	ctx, cancelAllocator := chromedp.NewExecAllocator(ctx,
		chromedp.NoDefaultBrowserCheck,
		chromedp.NoFirstRun,
		chromedp.IgnoreCertErrors,
		maybeHeadless,
	)
	defer cancelAllocator()

	ctx, cancel := chromedp.NewContext(ctx)
	defer cancel()

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		if do, ok := ev.(*page.EventJavascriptDialogOpening); ok {
			t.Logf("dialog opening: %v", do.Message)

			go func() {
				assert.Equal(t, tc.expectedDialogText, do.Message)
				assert.NoError(t, chromedp.Run(ctx, page.HandleJavaScriptDialog(tc.dialogResponse)))
				tc.expectedDialogText = ""
			}()
		}

		if ev, ok := ev.(*browser.EventDownloadProgress); ok {
			if ev.State == browser.DownloadProgressStateCompleted {
				tc.downloadFinished <- ev.GUID
			}
		}
	})

	run(ctx, &sp, &tc)

	if os.Getenv("HTMLUI_TEST_PAUSE") != "" {
		time.Sleep(time.Hour)
	}
}

func TestEndToEndTest(t *testing.T) {
	runInBrowser(t, func(ctx context.Context, sp *testutil.ServerParameters, tc *TestContext) {
		repoPath := testutil.TempDirectory(t)
		downloadDir := testutil.TempDirectory(t)
		snap1Path := testutil.TempDirectory(t)

		require.NoError(t, os.WriteFile(filepath.Join(snap1Path, "some-file.txt"), []byte("content"), 0o644))

		// navigate to the base page, wait unti we're redirected to 'Repository' page
		require.NoError(t, chromedp.Run(ctx,
			chromedp.Navigate(sp.BaseURL),
			chromedp.WaitVisible("button[data-testid='provider-filesystem']"),
			tc.captureScreenshot("initial"),

			tc.log("configuring filesystem provider"),
			chromedp.Click("button[data-testid='provider-filesystem']"),
			tc.captureScreenshot("filesystem-setup"),

			tc.log("entering repo path: "+repoPath),
			chromedp.SendKeys("input[data-testid='control-path']", repoPath+"\n"),

			tc.log("entering password"),
			chromedp.SendKeys("input[data-testid='control-password']", "password1"),
			chromedp.SendKeys("input[data-testid='control-confirmPassword']", "password1\n"),

			tc.log("waiting for snapshot list"),
			chromedp.WaitVisible(`a[data-testid='new-snapshot']`),
			tc.captureScreenshot("snapshot-list"),

			tc.log("clicking new snapshot"),
			chromedp.Click(`a[data-testid='new-snapshot']`),

			tc.log("entering path:"+snap1Path),
			chromedp.SendKeys(`input[name='path']`, snap1Path+"\t"),
			chromedp.Sleep(2*time.Second),

			tc.log("clicking estimate"),
			chromedp.Click(`button[data-testid='estimate-now']`),
			tc.captureScreenshot("estimate-clicked"),
			chromedp.Sleep(time.Second),
			tc.captureScreenshot("estimate-1s"),

			tc.log("clicking Snapshot Now"),
			chromedp.Click(`button[data-testid='snapshot-now']`),
			tc.captureScreenshot("snapshot-clicked"),

			tc.log("waiting for snapshot list"),
			chromedp.WaitVisible(`a[data-testid='new-snapshot']`),
			tc.captureScreenshot("snapshot-list"),

			tc.log("clicking on snapshot source"),
			chromedp.Click(`a[href*='/snapshots/single-source']`),
			tc.captureScreenshot("snapshot-source"),

			tc.log("clicking on snapshot dir"),
			chromedp.Click(`a[href*='/snapshots/dir/']`),
			tc.captureScreenshot("snapshot-dir"),

			tc.log("clicking on snapshot file"),
			browser.SetDownloadBehavior(browser.SetDownloadBehaviorBehaviorAllowAndName).
				WithDownloadPath(downloadDir).
				WithEventsEnabled(true),
			chromedp.Click(`a[href*='/api/v1/objects/']`),
			tc.waitForDownload(5*time.Second),

			tc.log("navigating to Repository page"),
			chromedp.Click("a[data-testid='tab-repo']"),
			tc.captureScreenshot("repository"),

			tc.log("disconnecting"),
			chromedp.Click("button[data-testid='disconnect']"),
			tc.captureScreenshot("disconnected"),

			tc.log("connecting"),
			chromedp.Click("button[data-testid='provider-filesystem']"),
			chromedp.SendKeys("input[data-testid='control-path']", repoPath+"\n"),
			chromedp.SendKeys("input[data-testid='control-password']", "password1\n"),

			tc.log("waiting for snapshot list"),
			chromedp.WaitVisible(`a[href*='/snapshots/new']`),
			tc.captureScreenshot("snapshot-list"),
		))
	})
}

func TestConnectDisconnectReconnect(t *testing.T) {
	runInBrowser(t, func(ctx context.Context, sp *testutil.ServerParameters, tc *TestContext) {
		repoPath := testutil.TempDirectory(t)

		// navigate to the base page, wait unti we're redirected to 'Repository' page
		require.NoError(t, chromedp.Run(ctx,
			chromedp.Navigate(sp.BaseURL),
			chromedp.WaitVisible("button[data-testid='provider-filesystem']"),
			tc.captureScreenshot("initial"),

			tc.log("configuring filesystem provider"),
			chromedp.Click("button[data-testid='provider-filesystem']"),
			tc.captureScreenshot("filesystem-setup"),

			tc.log("entering invalid password"),
			chromedp.SendKeys("input[data-testid='control-path']", repoPath+"\n"),
			chromedp.SendKeys("input[data-testid='control-password']", "password1"),
			tc.expectDialogText("Passwords don't match", false),
			chromedp.SendKeys("input[data-testid='control-confirmPassword']", "password2\n"),
			tc.captureScreenshot("invalid-password"),

			tc.log("fixing password"),
			chromedp.SendKeys("input[data-testid='control-confirmPassword']", kb.Backspace+"1\n"),

			tc.log("waiting for snapshot list"),
			chromedp.WaitVisible(`a[data-testid='new-snapshot']`),
			tc.captureScreenshot("snapshot-list"),

			tc.log("navigating to Repository page"),
			chromedp.Click("a[data-testid='tab-repo']"),
			tc.captureScreenshot("repository"),

			tc.log("disconnecting"),
			chromedp.Click("button[data-testid='disconnect']"),
			tc.captureScreenshot("disconnected"),

			tc.log("connecting"),
			chromedp.Click("button[data-testid='provider-filesystem']"),
			chromedp.SendKeys("input[data-testid='control-path']", repoPath+"\n"),
			chromedp.SendKeys("input[data-testid='control-password']", "password1\n"),

			tc.log("waiting for snapshot list"),
			chromedp.WaitVisible(`a[data-testid='new-snapshot']`),
			tc.captureScreenshot("snapshot-list"),
		))
	})
}

func TestDarkModeToggle(t *testing.T) {
	runInBrowser(t, func(ctx context.Context, sp *testutil.ServerParameters, tc *TestContext) {
		var nodes []*cdp.Node

		require.NoError(t, chromedp.Run(ctx,
			chromedp.Navigate(sp.BaseURL),
			chromedp.Nodes("html", &nodes),
		))

		// ensure we start with light mode
		if nodes[0].AttributeValue("class") == "dark" {
			require.NoError(t, chromedp.Run(ctx,
				chromedp.Click("button[data-testid='toggle-dark-mode']"),
			))
		}

		require.NoError(t, chromedp.Run(ctx,
			chromedp.WaitVisible("html.light"),
			chromedp.Click("button[data-testid='toggle-dark-mode']"),
			chromedp.WaitVisible("html.dark"),
			chromedp.Click("button[data-testid='toggle-dark-mode']"),
			chromedp.WaitVisible("html.light"),
			chromedp.Click("button[data-testid='toggle-dark-mode']"),
			chromedp.WaitVisible("html.dark"),
		))
	})
}
