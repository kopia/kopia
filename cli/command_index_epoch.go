package cli

type commandIndexEpoch struct {
	list        commandIndexEpochList
	mergeRanges commandIndexEpochMergeRanges
}

func (c *commandIndexEpoch) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("epoch", "Manage index manager epochs").Hidden()

	c.list.setup(svc, cmd)
	c.mergeRanges.setup(svc, cmd)
}
