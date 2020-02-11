package policy

import (
	"reflect"
	"testing"
)

func TestErrorHandlingPolicyMerge(t *testing.T) {
	type fields struct {
		IgnoreFileErrors      *bool
		IgnoreDirectoryErrors *bool
	}

	type args struct {
		src ErrorHandlingPolicy
	}

	for _, tt := range []struct {
		name      string
		fields    fields
		args      args
		expResult ErrorHandlingPolicy
	}{
		{
			name: "Policy being merged has no value set - expect no change",
			fields: fields{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      nil,
					IgnoreDirectoryErrors: nil,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
			},
		},
		{
			name: "Policy being merged has a value set at false - expect result to have value set at false",
			fields: fields{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newBool(false),
					IgnoreDirectoryErrors: newBool(false),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newBool(false),
				IgnoreDirectoryErrors: newBool(false),
			},
		},
		{
			name: "Policy being merged has a value set at true - expect result to have value set at true",
			fields: fields{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newBool(true),
					IgnoreDirectoryErrors: newBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newBool(true),
				IgnoreDirectoryErrors: newBool(true),
			},
		},
		{
			name: "Starting policy already has a value set at false - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:      newBool(false),
				IgnoreDirectoryErrors: newBool(false),
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newBool(true),
					IgnoreDirectoryErrors: newBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newBool(false),
				IgnoreDirectoryErrors: newBool(false),
			},
		},
		{
			name: "Policy being merged has a value set at true - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:      newBool(true),
				IgnoreDirectoryErrors: newBool(true),
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newBool(false),
					IgnoreDirectoryErrors: newBool(false),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newBool(true),
				IgnoreDirectoryErrors: newBool(true),
			},
		},
		{
			name: "Change just one param",
			fields: fields{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      nil,
					IgnoreDirectoryErrors: newBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: newBool(true),
			},
		},
	} {
		t.Log(tt.name)

		p := &ErrorHandlingPolicy{
			IgnoreFileErrors:      tt.fields.IgnoreFileErrors,
			IgnoreDirectoryErrors: tt.fields.IgnoreDirectoryErrors,
		}
		p.Merge(tt.args.src)

		if !reflect.DeepEqual(*p, tt.expResult) {
			t.Errorf("Policy after merge was not what was expected\n%v != %v", p, tt.expResult)
		}
	}
}

func TestErrorHandlingPolicy_IgnoreFileErrorsOrDefault(t *testing.T) {
	for _, tt := range []struct {
		name             string
		ignoreFileErrors *bool
		def              bool
		want             bool
	}{
		{
			name:             "ignoreFileErrors is nil, default is false",
			ignoreFileErrors: nil,
			def:              false,
			want:             false,
		},
		{
			name:             "ignoreFileErrors is false, default is false",
			ignoreFileErrors: newBool(false),
			def:              false,
			want:             false,
		},
		{
			name:             "ignoreFileErrors is true, default is false",
			ignoreFileErrors: newBool(true),
			def:              false,
			want:             true,
		},
		{
			name:             "ignoreFileErrors is nil, default is true",
			ignoreFileErrors: nil,
			def:              true,
			want:             true,
		},
		{
			name:             "ignoreFileErrors is false, default is true",
			ignoreFileErrors: newBool(false),
			def:              true,
			want:             false,
		},
		{
			name:             "ignoreFileErrors is true, default is true",
			ignoreFileErrors: newBool(true),
			def:              true,
			want:             true,
		},
	} {
		t.Log(tt.name)

		p := &ErrorHandlingPolicy{
			IgnoreFileErrors: tt.ignoreFileErrors,
		}

		if got := p.IgnoreFileErrorsOrDefault(tt.def); got != tt.want {
			t.Errorf("ErrorHandlingPolicy.IgnoreFileErrorsOrDefault() = %v, want %v", got, tt.want)
		}
	}
}

func TestErrorHandlingPolicy_IgnoreDirectoryErrorsOrDefault(t *testing.T) {
	for _, tt := range []struct {
		name            string
		ignoreDirErrors *bool
		def             bool
		want            bool
	}{
		{
			name:            "ignoreDirErrors is nil, default is false",
			ignoreDirErrors: nil,
			def:             false,
			want:            false,
		},
		{
			name:            "ignoreDirErrors is false, default is false",
			ignoreDirErrors: newBool(false),
			def:             false,
			want:            false,
		},
		{
			name:            "ignoreDirErrors is true, default is false",
			ignoreDirErrors: newBool(true),
			def:             false,
			want:            true,
		},
		{
			name:            "ignoreDirErrors is nil, default is true",
			ignoreDirErrors: nil,
			def:             true,
			want:            true,
		},
		{
			name:            "ignoreDirErrors is false, default is true",
			ignoreDirErrors: newBool(false),
			def:             true,
			want:            false,
		},
		{
			name:            "ignoreDirErrors is true, default is true",
			ignoreDirErrors: newBool(true),
			def:             true,
			want:            true,
		},
	} {
		t.Log(tt.name)

		p := &ErrorHandlingPolicy{
			IgnoreDirectoryErrors: tt.ignoreDirErrors,
		}

		if got := p.IgnoreDirectoryErrorsOrDefault(tt.def); got != tt.want {
			t.Errorf("ErrorHandlingPolicy.IgnoreDirectoryErrorsOrDefault() = %v, want %v", got, tt.want)
		}
	}
}
