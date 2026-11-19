package htmluie2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/pkg/errors"
)

type TestContext struct {
	t *testing.T

	snapshotCounter    int
	screenshotsDir     string
	expectedDialogText string
	dialogResponse     bool

	downloadFinished chan string
}

func (tc *TestContext) expectDialogText(txt string, respond bool) chromedp.Action[chromedp.Void] {
	return chromedp.Func(func(context.Context, *chromedp.Target) error {
		tc.expectedDialogText = txt
		tc.dialogResponse = respond

		return nil
	})
}

func (tc *TestContext) log(msg string) chromedp.Action[chromedp.Void] {
	return chromedp.Func(func(context.Context, *chromedp.Target) error {
		tc.t.Log(msg)
		return nil
	})
}

func (tc *TestContext) waitForDownload(waitTime time.Duration) chromedp.Action[chromedp.Void] {
	return chromedp.Func(func(context.Context, *chromedp.Target) error {
		// wait for download
		select {
		case <-tc.downloadFinished:
			tc.t.Logf("file downloaded, good!")

		case <-time.After(waitTime):
			return errors.New("download did not complete")
		}

		return nil
	})
}

func (tc *TestContext) captureScreenshot(fname string) chromedp.Action[chromedp.Void] {
	return chromedp.Func(func(ctx context.Context, target *chromedp.Target) error {
		b, err := chromedp.CaptureScreenshot()(ctx, target)
		if err != nil {
			return err
		}

		tc.snapshotCounter++

		return os.WriteFile(filepath.Join(tc.screenshotsDir, fmt.Sprintf("%04v.%v.png", tc.snapshotCounter, fname)), b, 0o600)
	})
}
