package cli

import (
	"fmt"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestListCommand = manifestCommands.Command("list", "List manifest items").Alias("ls").Hidden()
	manifestListPrefix  = manifestListCommand.Flag("prefix", "Prefix").String()
)

func init() {
	manifestListCommand.Action(listManifestItems)
}

func listManifestItems(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	items := rep.Manifests.Find(nil)

	for _, id := range items {
		var data map[string]interface{}

		labels, err := rep.Manifests.Get(id, &data)
		if err != nil {
			return err
		}

		t := labels["type"]
		delete(labels, "type")

		fmt.Printf("%v %v\n", id, t)
		for _, k := range sortedMapKeys(labels) {
			fmt.Printf("  %v: %v\n", k, labels[k])
		}
	}

	return nil
}

func sortedMapKeys(m map[string]string) []string {
	var result []string

	for k := range m {
		result = append(result, k)
	}

	sort.Strings(result)
	return result
}
