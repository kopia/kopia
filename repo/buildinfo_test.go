package repo

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRevisionString(t *testing.T) {
	cases := []struct {
		input []debug.BuildSetting
		want  string
	}{
		{
			want: "0.-(unknown_revision)",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.modified",
					Value: "true",
				},
			},
			want: "0.-(unknown_revision)+dirty",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
			},
			want: "0.2025-04-12T16:01:30Z-(unknown_revision)",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
				{
					Key:   "vcs.modified",
					Value: "true",
				},
			},
			want: "0.2025-04-12T16:01:30Z-(unknown_revision)+dirty",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
				{
					Key:   "vcs.revision",
					Value: "353676da445938316fa00b2b812a61f4b1dd3ffa",
				},
			},
			want: "0.2025-04-12T16:01:30Z-353676da4459",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
				{
					Key:   "vcs.revision",
					Value: "353676da4459",
				},
			},
			want: "0.2025-04-12T16:01:30Z-353676da4459",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
				{
					Key:   "vcs.revision",
					Value: "353676da",
				},
			},
			want: "0.2025-04-12T16:01:30Z-353676da",
		},
		{
			input: []debug.BuildSetting{
				{
					Key:   "vcs.time",
					Value: "2025-04-12T16:01:30Z",
				},
				{
					Key:   "vcs.revision",
					Value: "353676da445938316fa00b2b812a61f4b1dd3ffa",
				},
				{
					Key:   "vcs.modified",
					Value: "true",
				},
			},
			want: "0.2025-04-12T16:01:30Z-353676da4459+dirty",
		},
	}

	for _, c := range cases {
		got := getRevisionString(c.input)
		require.Equal(t, c.want, got)
	}
}
