package notification

import "github.com/kopia/kopia/notification/notifydata"

// ReportSeverity returns the notification severity for the given multi-snapshot status.
func ReportSeverity(st notifydata.MultiSnapshotStatus) Severity {
	switch st.OverallStatusCode() {
	case notifydata.StatusCodeFatal:
		return SeverityError
	case notifydata.StatusCodeWarnings:
		return SeverityWarning
	default:
		return SeverityReport
	}
}
