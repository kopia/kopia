package uitask

// CounterValue describes the counter value reported by task with optional units for presentation.
type CounterValue struct {
	Value int64  `json:"value"`
	Units string `json:"units,omitempty"`
	Level string `json:"level"` // "", "notice", "warning" or "error"
}

// BytesCounter returns CounterValue for the number of bytes.
func BytesCounter(v int64) CounterValue {
	return CounterValue{v, "bytes", ""}
}

// SimpleCounter returns simple numeric CounterValue without units.
func SimpleCounter(v int64) CounterValue {
	return CounterValue{v, "", ""}
}

// NoticeBytesCounter returns CounterValue for the number of bytes.
func NoticeBytesCounter(v int64) CounterValue {
	return CounterValue{v, "bytes", "notice"}
}

// NoticeCounter returns simple numeric CounterValue without units.
func NoticeCounter(v int64) CounterValue {
	return CounterValue{v, "", "notice"}
}

// WarningBytesCounter returns CounterValue for the number of bytes.
func WarningBytesCounter(v int64) CounterValue {
	return CounterValue{v, "bytes", "warning"}
}

// WarningCounter returns simple numeric CounterValue without units.
func WarningCounter(v int64) CounterValue {
	return CounterValue{v, "", "warning"}
}

// ErrorBytesCounter returns CounterValue for the number of bytes.
func ErrorBytesCounter(v int64) CounterValue {
	return CounterValue{v, "bytes", "error"}
}

// ErrorCounter returns simple numeric CounterValue without units.
func ErrorCounter(v int64) CounterValue {
	return CounterValue{v, "", "error"}
}

func cloneCounters(c map[string]CounterValue) map[string]CounterValue {
	newCounters := map[string]CounterValue{}
	for k, v := range c {
		newCounters[k] = v
	}

	return newCounters
}
