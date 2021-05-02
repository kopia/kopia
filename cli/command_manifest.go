package cli

type commandManifest struct {
	delete commandManifestDelete
	list   commandManifestList
	show   commandManifestShow
}

func (c *commandManifest) setup(app appServices, parent commandParent) {
	cmd := parent.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()

	c.delete.setup(app, cmd)
	c.list.setup(app, cmd)
	c.show.setup(app, cmd)
}
