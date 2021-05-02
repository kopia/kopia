package cli

type commandManifest struct {
	delete commandManifestDelete
	list   commandManifestList
	show   commandManifestShow
}

func (c *commandManifest) setup(parent commandParent) {
	cmd := parent.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()

	c.delete.setup(cmd)
	c.list.setup(cmd)
	c.show.setup(cmd)
}
