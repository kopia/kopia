package policy

import (
	"reflect"
	"testing"

	"github.com/kopia/kopia/snapshot"
)

func TestErrorHandlingPolicyMerge(t *testing.T) {
	type fields struct {
		IgnoreFileErrors      *OptionalBool
		IgnoreDirectoryErrors *OptionalBool
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
					IgnoreFileErrors:      newOptionalBool(false),
					IgnoreDirectoryErrors: newOptionalBool(false),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(false),
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
					IgnoreFileErrors:      newOptionalBool(true),
					IgnoreDirectoryErrors: newOptionalBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
		},
		{
			name: "Starting policy already has a value set at false - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newOptionalBool(true),
					IgnoreDirectoryErrors: newOptionalBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
		},
		{
			name: "Policy being merged has a value set at true - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:      newOptionalBool(false),
					IgnoreDirectoryErrors: newOptionalBool(false),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
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
					IgnoreDirectoryErrors: newOptionalBool(true),
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
		},
	} {
		t.Log(tt.name)

		p := &ErrorHandlingPolicy{
			IgnoreFileErrors:      tt.fields.IgnoreFileErrors,
			IgnoreDirectoryErrors: tt.fields.IgnoreDirectoryErrors,
		}

		p.Merge(tt.args.src, &ErrorHandlingPolicyDefinition{}, snapshot.SourceInfo{})

		if !reflect.DeepEqual(*p, tt.expResult) {
			t.Errorf("Policy after merge was not what was expected\n%v != %v", p, tt.expResult)
		}
	}
}
