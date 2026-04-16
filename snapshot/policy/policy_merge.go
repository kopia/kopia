package policy

import (
	"sort"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/snapshot"
)

// MergePolicies computes the policy by applying the specified list of policies in order from most
// specific to most general.
func MergePolicies(policies []*Policy, si snapshot.SourceInfo) (*Policy, *Definition) {
	var (
		merged Policy
		def    Definition
	)

	merged.Labels = LabelsForSource(si)

	for _, p := range policies {
		merged.RetentionPolicy.Merge(p.RetentionPolicy, &def.RetentionPolicy, p.Target())
		merged.FilesPolicy.Merge(p.FilesPolicy, &def.FilesPolicy, p.Target())
		merged.ErrorHandlingPolicy.Merge(p.ErrorHandlingPolicy, &def.ErrorHandlingPolicy, p.Target())
		merged.SchedulingPolicy.Merge(p.SchedulingPolicy, &def.SchedulingPolicy, p.Target())
		merged.UploadPolicy.Merge(p.UploadPolicy, &def.UploadPolicy, p.Target())
		merged.CompressionPolicy.Merge(p.CompressionPolicy, &def.CompressionPolicy, p.Target())
		merged.MetadataCompressionPolicy.Merge(p.MetadataCompressionPolicy, &def.MetadataCompressionPolicy, p.Target())
		merged.SplitterPolicy.Merge(p.SplitterPolicy, &def.SplitterPolicy, p.Target())
		merged.Actions.Merge(p.Actions, &def.Actions, p.Target())
		merged.OSSnapshotPolicy.Merge(p.OSSnapshotPolicy, &def.OSSnapshotPolicy, p.Target())
		merged.LoggingPolicy.Merge(p.LoggingPolicy, &def.LoggingPolicy, p.Target())

		if p.NoParent {
			return &merged, &def
		}
	}

	// Merge default expiration policy.
	merged.RetentionPolicy.Merge(defaultRetentionPolicy, &def.RetentionPolicy, GlobalPolicySourceInfo)
	merged.FilesPolicy.Merge(defaultFilesPolicy, &def.FilesPolicy, GlobalPolicySourceInfo)
	merged.ErrorHandlingPolicy.Merge(defaultErrorHandlingPolicy, &def.ErrorHandlingPolicy, GlobalPolicySourceInfo)
	merged.SchedulingPolicy.Merge(defaultSchedulingPolicy, &def.SchedulingPolicy, GlobalPolicySourceInfo)
	merged.UploadPolicy.Merge(defaultUploadPolicy, &def.UploadPolicy, GlobalPolicySourceInfo)
	merged.CompressionPolicy.Merge(defaultCompressionPolicy, &def.CompressionPolicy, GlobalPolicySourceInfo)
	merged.MetadataCompressionPolicy.Merge(defaultMetadataCompressionPolicy, &def.MetadataCompressionPolicy, GlobalPolicySourceInfo)
	merged.SplitterPolicy.Merge(defaultSplitterPolicy, &def.SplitterPolicy, GlobalPolicySourceInfo)
	merged.Actions.Merge(defaultActionsPolicy, &def.Actions, GlobalPolicySourceInfo)
	merged.OSSnapshotPolicy.Merge(defaultOSSnapshotPolicy, &def.OSSnapshotPolicy, GlobalPolicySourceInfo)
	merged.LoggingPolicy.Merge(defaultLoggingPolicy, &def.LoggingPolicy, GlobalPolicySourceInfo)

	if len(policies) > 0 {
		merged.Actions.MergeNonInheritable(policies[0].Actions)
	}

	return &merged, &def
}

func mergeOptionalBool(target **OptionalBool, src *OptionalBool, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		v := *src

		*target = &v
		*def = si
	}
}

func mergeOptionalInt(target **OptionalInt, src *OptionalInt, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		v := *src

		*target = &v
		*def = si
	}
}

func mergeOptionalInt64(target **OptionalInt64, src *OptionalInt64, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		v := *src

		*target = &v
		*def = si
	}
}

func mergeStringsReplace(target *[]string, src []string, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if len(*target) == 0 && len(src) > 0 {
		*target = src
		*def = si
	}
}

func mergeStrings(target *[]string, targetNoParent *bool, src []string, noParent bool, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *targetNoParent {
		// merges prevented
		return
	}

	merged := map[string]bool{}

	for _, v := range *target {
		merged[v] = true
	}

	for _, v := range src {
		merged[v] = true
		*def = si
	}

	var result []string
	for v := range merged {
		result = append(result, v)
	}

	sort.Strings(result)

	*target = result

	if noParent {
		// prevent future merges.
		*targetNoParent = noParent
	}
}

func mergeString(target *string, src string, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == "" && src != "" {
		*target = src
		*def = si
	}
}

func mergeCompressionName(target *compression.Name, src compression.Name, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == "" && src != "" {
		*target = src
		*def = si
	}
}

func mergeInt64(target *int64, src int64, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == 0 && src != 0 {
		*target = src
		*def = si
	}
}

func mergeBool(target *bool, src bool, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if !*target && src {
		*target = src
		*def = si
	}
}

func mergeStringList(target *[]string, src []string, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if len(*target) == 0 && len(src) != 0 {
		*target = src
		*def = si
	}
}

func mergeLogLevel(target **LogDetail, src *LogDetail, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		b := *src

		*target = &b
		*def = si
	}
}

func mergeActionCommand(target **ActionCommand, src *ActionCommand, def *snapshot.SourceInfo, si snapshot.SourceInfo) {
	if *target == nil && src != nil {
		b := *src

		*target = &b
		*def = si
	}
}
