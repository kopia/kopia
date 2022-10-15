package units

import (
	"os"
	"testing"
)

var base10Cases = []struct {
	value    int64
	expected string
}{
	{0, "0 B"},
	{1, "1 B"},
	{2, "2 B"},
	{899, "899 B"},
	{900, "0.9 KB"},
	{999, "1 KB"},
	{1000, "1 KB"},
	{1200, "1.2 KB"},
	{899999, "900 KB"},
	{900000, "0.9 MB"},
	{999000, "1 MB"},
	{999999, "1 MB"},
	{1000000, "1 MB"},
	{99000000, "99 MB"},
	{990000000, "1 GB"},
	{9990000000, "10 GB"},
	{99900000000, "99.9 GB"},
	{1000000000000, "1 TB"},
	{99000000000000, "99 TB"},
}

var base2Cases = []struct {
	value    int64
	expected string
}{
	{0, "0 B"},
	{1, "1 B"},
	{2, "2 B"},
	{899, "899 B"},
	{900, "900 B"},
	{999, "1 KiB"},
	{1024, "1 KiB"},
	{1400, "1.4 KiB"},
	{900<<10 - 1, "900 KiB"},
	{900 << 10, "900 KiB"},
	{999000, "1 MiB"},
	{999999, "1 MiB"},
	{1000000, "1 MiB"},
	{99 << 20, "99 MiB"},
	{1 << 30, "1 GiB"},
	{10 << 30, "10 GiB"},
	{99900000000, "93 GiB"},
	{1000000000000, "0.9 TiB"},
	{99000000000000, "90 TiB"},
}

func TestBytesStringBase10(t *testing.T) {
	for i, c := range base10Cases {
		actual := BytesStringBase10(c.value)
		if actual != c.expected {
			t.Errorf("case #%v failed for %v, expected: '%v', got '%v'", i, c.value, c.expected, actual)
		}
	}
}

func TestBytesStringBase2(t *testing.T) {
	for i, c := range base2Cases {
		actual := BytesStringBase2(c.value)
		if actual != c.expected {
			t.Errorf("case #%v failed for %v, expected: '%v', got '%v'", i, c.value, c.expected, actual)
		}
	}
}

func TestBytesString(t *testing.T) {
	defer os.Unsetenv(bytesStringBase2Envar)

	os.Setenv(bytesStringBase2Envar, "false")
	for i, c := range base10Cases {
		actual := BytesString(c.value)
		if actual != c.expected {
			t.Errorf("case #%v failed for %v, expected: '%v', got '%v'", i, c.value, c.expected, actual)
		}
	}

	os.Setenv(bytesStringBase2Envar, "true")
	for i, c := range base2Cases {
		actual := BytesString(c.value)
		if actual != c.expected {
			t.Errorf("case #%v failed for %v, expected: '%v', got '%v'", i, c.value, c.expected, actual)
		}
	}
}
