package policy

// LogDetail represents the details of log output.
type LogDetail int

// Supported log detail levels.
const (
	LogDetailNone   LogDetail = 0
	LogDetailNormal LogDetail = 5
	LogDetailMax    LogDetail = 10
)

// OrDefault returns the log detail or the provided default.
func (l *LogDetail) OrDefault(def LogDetail) LogDetail {
	if l == nil {
		return def
	}

	return *l
}

// NewLogDetail returns a pointer to the provided LogDetail.
func NewLogDetail(l LogDetail) *LogDetail {
	return &l
}
