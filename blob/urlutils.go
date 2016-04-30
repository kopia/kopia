package blob

import (
	"net/url"
	"os"
	"strconv"
)

func getIntPtrValue(u *url.URL, name string, base int) *int {
	if value := u.Query().Get(name); value != "" {
		if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
			intVal := int(int64Val)
			return &intVal
		}
	}

	return nil
}

func getFileModeValue(u *url.URL, name string, def os.FileMode) os.FileMode {
	if value := u.Query().Get(name); value != "" {
		if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
			return os.FileMode(uint32Val)
		}
	}

	return def
}

func getStringValue(u *url.URL, name string, def string) string {
	if value := u.Query().Get(name); value != "" {
		return value
	}

	return def
}
