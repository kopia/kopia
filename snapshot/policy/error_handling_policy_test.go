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
