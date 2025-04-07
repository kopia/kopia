package policy_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var omittedDefinitionFields = map[string]bool{
	"Definition.NoParent":                               true, // special
	"Definition.Labels":                                 true, // special
	"ActionsPolicyDefinition.BeforeFolder":              true, // non-inheritable field
	"ActionsPolicyDefinition.AfterFolder":               true, // non-inheritable field
	"SchedulingPolicyDefinition.NoParentTimesOfDay":     true, // special
	"CompressionPolicyDefinition.NoParentOnlyCompress":  true,
	"CompressionPolicyDefinition.NoParentNeverCompress": true,
}

func TestPolicyDefinition(t *testing.T) {
	// verify that each field in the policy struct recursively matches a corresponding field
	// from the policy.Definition() struct.
	ensureTypesMatch(t, reflect.TypeOf(policy.Policy{}), reflect.TypeOf(policy.Definition{}))
}

func ensureTypesMatch(t *testing.T, policyType, definitionType reflect.Type) {
	t.Helper()

	sourceInfoType := reflect.TypeOf(snapshot.SourceInfo{})

	for i := range policyType.NumField() {
		f := policyType.Field(i)

		dt, ok := definitionType.FieldByName(f.Name)
		if !ok {
			require.True(t, omittedDefinitionFields[definitionType.Name()+"."+f.Name], "definition field %q not found in %q", f.Name, definitionType.Name())
			continue
		}

		t.Logf("f: %v %v", definitionType.Name(), f.Name)

		if f.Type.Kind() == reflect.Struct {
			ensureTypesMatch(t, f.Type, dt.Type)
		} else {
			require.True(t, sourceInfoType.AssignableTo(dt.Type), "invalid type of %v.%v - %v", definitionType.Name(), dt.Name, dt.Type)
		}

		require.Equal(t, f.Tag.Get("json"), dt.Tag.Get("json"), dt.Name)
	}
}

func TestPolicyMerge(t *testing.T) {
	testPolicyMerge(t, reflect.TypeOf(policy.Policy{}), reflect.TypeOf(policy.Definition{}), "")
}

//nolint:thelper
func testPolicyMerge(t *testing.T, policyType, definitionType reflect.Type, prefix string) {
	for i := range policyType.NumField() {
		f := policyType.Field(i)

		dt, ok := definitionType.FieldByName(f.Name)
		if !ok {
			continue
		}

		if f.Type.Kind() == reflect.Struct {
			testPolicyMerge(t, f.Type, dt.Type, prefix+f.Name+".")

			continue
		}

		t.Run(prefix+f.Name, func(t *testing.T) {
			testPolicyMergeSingleField(t, prefix+f.Name, f.Type)
		})
	}
}

//nolint:thelper
func testPolicyMergeSingleField(t *testing.T, fieldName string, typ reflect.Type) {
	var v0, v1, v2 reflect.Value

	switch typ.String() {
	case "*int":
		i1 := 111
		i2 := 222

		v0 = reflect.ValueOf((*int)(nil))
		v1 = reflect.ValueOf(&i1)
		v2 = reflect.ValueOf(&i2)

	case "[]string":
		s1 := []string{"aa"}
		s2 := []string{"bb", "cc"}

		v0 = reflect.ValueOf([]string{})
		v1 = reflect.ValueOf(s1)
		v2 = reflect.ValueOf(s2)

	case "*policy.OptionalBool":
		ob1 := policy.OptionalBool(false)
		ob2 := policy.OptionalBool(true)

		v0 = reflect.ValueOf((*policy.OptionalBool)(nil))
		v1 = reflect.ValueOf(&ob1)
		v2 = reflect.ValueOf(&ob2)

	case "*policy.OptionalInt":
		ob1 := policy.OptionalInt(1)
		ob2 := policy.OptionalInt(7)

		v0 = reflect.ValueOf((*policy.OptionalInt)(nil))
		v1 = reflect.ValueOf(&ob1)
		v2 = reflect.ValueOf(&ob2)

	case "*policy.OptionalInt64":
		ob1 := policy.OptionalInt64(1)
		ob2 := policy.OptionalInt64(7)

		v0 = reflect.ValueOf((*policy.OptionalInt64)(nil))
		v1 = reflect.ValueOf(&ob1)
		v2 = reflect.ValueOf(&ob2)

	case "bool":
		v0 = reflect.ValueOf(false)
		v1 = reflect.ValueOf(false)
		v2 = reflect.ValueOf(true)
	case "int64":
		v0 = reflect.ValueOf(int64(0))
		v1 = reflect.ValueOf(int64(1))
		v2 = reflect.ValueOf(int64(2))
	case "*policy.LogDetail":
		ld1 := policy.LogDetail(1)
		ld2 := policy.LogDetail(2)

		v0 = reflect.ValueOf((*policy.LogDetail)(nil))
		v1 = reflect.ValueOf(&ld1)
		v2 = reflect.ValueOf(&ld2)
	case "*policy.ActionCommand":
		v0 = reflect.ValueOf((*policy.ActionCommand)(nil))
		v1 = reflect.ValueOf(&policy.ActionCommand{Command: "foo"})
		v2 = reflect.ValueOf(&policy.ActionCommand{Command: "bar"})
	case "[]policy.TimeOfDay":
		v0 = reflect.ValueOf([]policy.TimeOfDay{})
		v1 = reflect.ValueOf([]policy.TimeOfDay{{Hour: 10}})
		v2 = reflect.ValueOf([]policy.TimeOfDay{{Hour: 11}})
	case "compression.Name":
		v0 = reflect.ValueOf(compression.Name(""))
		v1 = reflect.ValueOf(compression.Name("foo"))
		v2 = reflect.ValueOf(compression.Name("bar"))
	case "*policy.OSSnapshotMode":
		v0 = reflect.ValueOf((*policy.OSSnapshotMode)(nil))
		v1 = reflect.ValueOf(policy.NewOSSnapshotMode(policy.OSSnapshotNever))
		v2 = reflect.ValueOf(policy.NewOSSnapshotMode(policy.OSSnapshotAlways))
	case "string":
		v0 = reflect.ValueOf("")
		v1 = reflect.ValueOf("FIXED-2M")
		v2 = reflect.ValueOf("FIXED-4M")

	default:
		t.Fatalf("unhandled case: %v - %v - please update test", fieldName, typ)
	}

	pol0 := policyWithField(fieldName, v0)
	pol1 := policyWithField(fieldName, v1)
	pol2 := policyWithField(fieldName, v2)

	pol0.Labels = map[string]string{
		"hostname": "host",
		"username": "user",
		"path":     "/xx",
	}

	pol1.Labels = map[string]string{
		"hostname": "host",
		"username": "user",
		"path":     "/xx/aa",
	}

	pol2.Labels = map[string]string{
		"hostname": "host",
		"username": "user",
		"path":     "/xx/bb",
	}

	tmp := *policy.DefaultPolicy
	defaultPolicyMod := &tmp

	disableParentMerging(defaultPolicyMod)

	// merging default policy with no-op policy results in default policy
	result, _ := policy.MergePolicies([]*policy.Policy{pol0, defaultPolicyMod}, pol0.Target())
	require.Equal(t, defaultPolicyMod.String(), result.String())

	result, _ = policy.MergePolicies([]*policy.Policy{pol1, pol0, defaultPolicyMod}, pol1.Target())
	require.Equal(t, pol1.String(), result.String())

	result, _ = policy.MergePolicies([]*policy.Policy{pol2, pol1, pol0, defaultPolicyMod}, pol2.Target())
	require.Equal(t, pol2.String(), result.String())

	result, _ = policy.MergePolicies([]*policy.Policy{pol2, pol0, defaultPolicyMod}, pol2.Target())
	require.Equal(t, pol2.String(), result.String())
}

func policyWithField(fname string, val reflect.Value) *policy.Policy {
	pol := *policy.DefaultPolicy
	p := &pol

	disableParentMerging(p)
	cur := reflect.ValueOf(p).Elem()

	parts := strings.Split(fname, ".")
	for _, part := range parts[:len(parts)-1] {
		cur = cur.FieldByName(part)
	}

	cur.FieldByName(parts[len(parts)-1]).Set(val)

	return p
}

func disableParentMerging(p *policy.Policy) {
	// set flags that disable merging of values with the parent, we will
	// test those cases separately.
	p.CompressionPolicy.NoParentNeverCompress = true
	p.CompressionPolicy.NoParentOnlyCompress = true
	p.SchedulingPolicy.NoParentTimesOfDay = true
}

func TestPolicyMergeOnlyCompressIncludingParents(t *testing.T) {
	p0 := &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			OnlyCompress: []string{"a", "c", "e"},
		},
	}

	p1 := &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			OnlyCompress: []string{"b", "d"},
		},
	}

	p2 := &policy.Policy{
		CompressionPolicy: policy.CompressionPolicy{
			OnlyCompress: []string{"f", "g"},
		},
	}

	result, _ := policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want := *policy.DefaultPolicy
	want.CompressionPolicy.OnlyCompress = []string{"a", "b", "c", "d", "e", "f", "g"}

	require.Equal(t, want.String(), result.String())

	p2.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.CompressionPolicy.OnlyCompress = []string{"a", "b", "c", "d", "e", "f", "g"}

	require.Equal(t, want.String(), result.String())

	p1.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.CompressionPolicy.OnlyCompress = []string{"a", "b", "c", "d", "e"}

	require.Equal(t, want.String(), result.String())

	p0.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.CompressionPolicy.OnlyCompress = []string{"a", "c", "e"}

	require.Equal(t, want.String(), result.String())
}

func TestPolicyMergeTimesOfDayIncludingParents(t *testing.T) {
	tod0 := policy.TimeOfDay{Hour: 10}
	tod1 := policy.TimeOfDay{Hour: 11}
	tod2 := policy.TimeOfDay{Hour: 12}
	tod3 := policy.TimeOfDay{Hour: 13}
	tod4 := policy.TimeOfDay{Hour: 14}
	tod5 := policy.TimeOfDay{Hour: 15}
	tod6 := policy.TimeOfDay{Hour: 16}

	p0 := &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			TimesOfDay: []policy.TimeOfDay{tod0, tod2, tod4},
		},
	}

	p1 := &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			TimesOfDay: []policy.TimeOfDay{tod1, tod3},
		},
	}

	p2 := &policy.Policy{
		SchedulingPolicy: policy.SchedulingPolicy{
			TimesOfDay: []policy.TimeOfDay{tod5, tod6},
		},
	}

	result, _ := policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want := *policy.DefaultPolicy
	want.SchedulingPolicy.TimesOfDay = []policy.TimeOfDay{tod0, tod1, tod2, tod3, tod4, tod5, tod6}

	require.Equal(t, want.String(), result.String())

	p2.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.SchedulingPolicy.TimesOfDay = []policy.TimeOfDay{tod0, tod1, tod2, tod3, tod4, tod5, tod6}

	require.Equal(t, want.String(), result.String())

	p1.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.SchedulingPolicy.TimesOfDay = []policy.TimeOfDay{tod0, tod1, tod2, tod3, tod4}

	require.Equal(t, want.String(), result.String())

	p0.NoParent = true
	result, _ = policy.MergePolicies([]*policy.Policy{p0, p1, p2}, p0.Target())

	want = policy.Policy{}
	want.SchedulingPolicy.TimesOfDay = []policy.TimeOfDay{tod0, tod2, tod4}

	require.Equal(t, want.String(), result.String())
}
