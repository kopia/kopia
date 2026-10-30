package policy_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/snapshot/policy"
)

func TestLogDetail(t *testing.T) {
	var s1, s2 struct {
		V0 policy.LogDetail  `json:"v0,omitempty"`
		V1 *policy.LogDetail `json:"v1,omitempty"`
		V2 policy.LogDetail  `json:"v2,omitempty"`
		V3 policy.LogDetail  `json:"v3,omitempty"`
	}

	s1.V1 = policy.NewLogDetail(policy.LogDetailNone)
	s1.V2 = policy.LogDetailNormal
	s1.V3 = policy.LogDetailMax

	v, err := json.Marshal(s1)
	require.NoError(t, err)
	require.Equal(t, `{"v1":0,"v2":5,"v3":10}`, string(v))

	require.NoError(t, json.Unmarshal(v, &s2))
	require.Equal(t, s1, s2)
}
