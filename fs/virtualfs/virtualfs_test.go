package virtualfs

import (
	"reflect"
	"testing"
)

func TestNewDirectory(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		rootName string
		expErr   bool
	}{
		{
			desc:     "Root Directory success",
			rootName: "root",
			expErr:   false,
		},
		{
			desc:     "Root directory with `/` in the name",
			rootName: "/root",
			expErr:   true,
		},
	} {
		t.Log(tc.desc)

		r, err := NewDirectory(tc.rootName)
		if tc.expErr {
			if err == nil {
				t.Errorf("expected error but got none")
			}
		} else {
			if err != nil {
				t.Errorf("expected success but got err: %v", err)
				continue
			}
			if r == nil {
				t.Errorf("expected root directory, got nil")
				continue
			}
			if !reflect.DeepEqual(r.Name(), tc.rootName) {
				t.Errorf("did not get expected output: (actual) %v != %v (expected)", r.Name(), tc.rootName)
			}
		}
	}
}
