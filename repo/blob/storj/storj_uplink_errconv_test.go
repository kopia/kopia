package storj

import (
	"errors"
	"testing"

	"storj.io/uplink"

	"github.com/kopia/kopia/repo/blob"
)

func Test_convertKnownError(t *testing.T) {
	type args struct {
		uplinkErr error
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "uplinkObjectNotFound",
			args:    args{uplinkErr: uplink.ErrObjectNotFound},
			wantErr: blob.ErrBlobNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := convertKnownError(tt.args.uplinkErr); !errors.Is(err, tt.wantErr) {
				t.Errorf("convertKnownError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
