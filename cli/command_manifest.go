package cli

type commandManifest struct {
	delete commandManifestDelete
	list   commandManifestList
	show   commandManifestShow
}

func (c *commandManifest) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("manifest", "Low-level commands to manipulate manifest items.").Hidden()

	c.delete.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.show.setup(svc, cmd)
}
