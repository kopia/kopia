package cli

type commandBlobShards struct {
	modify commandBlobShardsModify
}

func (c *commandBlobShards) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("shards", "Manipulate shards in a blob store").Hidden()

	c.modify.setup(svc, cmd)
}
