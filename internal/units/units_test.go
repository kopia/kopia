package units

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var base10Cases = []struct {
	value    uint64
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
	{900000000000000 - 1, "900 TB"},
	{900000000000000, "0.9 PB"},
	{990000000000000, "1 PB"},
	{1000000000000000, "1 PB"},
	{800000000000000000, "800 PB"},
	{900000000000000000, "0.9 EB"},
	{990000000000000000, "1 EB"},
	{1000000000000000000, "1 EB"},
	{10000000000000000000, "10 EB"},
}

var base2Cases = []struct {
	value    uint64
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
	{1000000000000, "0.9 TiB"},
	{1 << 40, "1 TiB"},
	{10 << 40, "10 TiB"},
	{99000000000000, "90 TiB"},
	{900 << 40, "900 TiB"},
	{(950 << 40), "0.9 PiB"},
	{990 << 40, "1 PiB"},

	{1 << 50, "1 PiB"},
	{10 << 50, "10 PiB"},
	{90 << 50, "90 PiB"},
	{900 << 50, "900 PiB"},
	{(950 << 50), "0.9 EiB"},
	{990 << 50, "1 EiB"},
	{1 << 60, "1 EiB"},
	{10 << 60, "10 EiB"},
	{15 << 60, "15 EiB"},
}

func TestBytesStringBase10(t *testing.T) {
	for i, c := range base10Cases {
		t.Run(fmt.Sprint(i, "-", c.value), func(t *testing.T) {
			actual := BytesStringBase10(c.value)
			require.Equal(t, c.expected, actual)
		})
	}
}

func TestBytesStringBase2(t *testing.T) {
	for i, c := range base2Cases {
		t.Run(fmt.Sprint(i, "-", c.value), func(t *testing.T) {
			actual := BytesStringBase2(c.value)
			require.Equal(t, c.expected, actual)
		})
	}
}

func TestBytesString_base2EnvFalse(t *testing.T) {
	t.Setenv(bytesStringBase2Envar, "false")

	for i, c := range base10Cases {
		t.Run(fmt.Sprint(i, "-", c.value), func(t *testing.T) {
			actual := BytesString(c.value)
			require.Equal(t, c.expected, actual)
		})
	}
}

func TestBytesString_base2EnvTrue(t *testing.T) {
	t.Setenv(bytesStringBase2Envar, "true")

	for i, c := range base2Cases {
		t.Run(fmt.Sprint(i, "-", c.value), func(t *testing.T) {
			actual := BytesString(c.value)
			require.Equal(t, c.expected, actual)
		})
	}
}
