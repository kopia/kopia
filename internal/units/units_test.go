package units

import "testing"

func TestToDecimalUnitString(t *testing.T) {
	cases := []struct {
		value    float64
		suffix   string
		expected string
	}{
		{0, "B", "0 B"},
		{1, "B", "1 B"},
		{2, "B", "2 B"},
		{899, "B", "899 B"},
		{900, "B", "0.9 KB"},
		{999, "B", "1 KB"},
		{1000, "B", "1 KB"},
		{1200, "B", "1.2 KB"},
		{899999, "B", "900 KB"},
		{900000, "B", "0.9 MB"},
		{999000, "B", "1 MB"},
		{999999, "B", "1 MB"},
		{1000000, "B", "1 MB"},
		{99000000, "B", "99 MB"},
		{990000000, "B", "1 GB"},
		{9990000000, "B", "10 GB"},
		{99900000000, "B", "99.9 GB"},
		{1000000000000, "B", "1 TB"},
		{99000000000000, "B", "99 TB"},
		{98765432109876543912, "B", "98765.4 TB"},
	}

	for i, c := range cases {
		actual := toDecimalUnitString(c.value, c.suffix)
		if actual != c.expected {
			t.Errorf("case #%v failed, expected: '%v', got '%v'", i, c.expected, actual)
		}
	}
}
