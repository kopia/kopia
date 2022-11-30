package acl_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/acl"
)

func TestAccessLevelJSONSerialization(t *testing.T) {
	var s1, s2 struct {
		B acl.AccessLevel `json:"b"`
		C acl.AccessLevel `json:"c"`
		D acl.AccessLevel `json:"d"`
		E acl.AccessLevel `json:"e"`
	}

	s1.B = acl.AccessLevelNone
	s1.C = acl.AccessLevelRead
	s1.D = acl.AccessLevelAppend
	s1.E = acl.AccessLevelFull

	v, err := json.MarshalIndent(s1, "", "  ")
	require.NoError(t, err)

	got := string(v)
	want := `{
  "b": "NONE",
  "c": "READ",
  "d": "APPEND",
  "e": "FULL"
}`

	if diff := cmp.Diff(got, want); diff != "" {
		t.Fatalf("diff: (-got, +want): %v", diff)
	}

	if err := json.Unmarshal(v, &s2); err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(s1, s2); diff != "" {
		t.Fatalf("diff: (-got, +want): %v", diff)
	}
}
