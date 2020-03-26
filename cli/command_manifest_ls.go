package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	manifestListCommand = manifestCommands.Command("list", "List manifest items").Alias("ls").Default()
	manifestListFilter  = manifestListCommand.Flag("filter", "List of key:value pairs").Strings()
	manifestListSort    = manifestListCommand.Flag("sort", "List of keys to sort by").Strings()
)

func init() {
	manifestListCommand.Action(repositoryAction(listManifestItems))
}

func listManifestItems(ctx context.Context, rep repo.Repository) error {
	filter := map[string]string{}

	for _, kv := range *manifestListFilter {
		p := strings.Index(kv, ":")
		if p <= 0 {
			return errors.Errorf("invalid list filter %q, missing ':'", kv)
		}

		filter[kv[0:p]] = kv[p+1:]
	}

	items, err := rep.FindManifests(ctx, filter)
	if err != nil {
		return err
	}

	sort.Slice(items, func(i, j int) bool {
		for _, key := range *manifestListSort {
			if v1, v2 := items[i].Labels[key], items[j].Labels[key]; v1 != v2 {
				return v1 < v2
			}
		}

		return items[i].ModTime.Before(items[j].ModTime)
	})

	for _, it := range items {
		t := it.Labels["type"]
		fmt.Printf("%v %10v %v type:%v %v\n", it.ID, it.Length, formatTimestamp(it.ModTime.Local()), t, sortedMapValues(it.Labels))
	}

	return nil
}

func sortedMapValues(m map[string]string) string {
	var result []string

	for k, v := range m {
		if k == "type" {
			continue
		}

		result = append(result, fmt.Sprintf("%v:%v", k, v))
	}

	sort.Strings(result)

	return strings.Join(result, " ")
}
