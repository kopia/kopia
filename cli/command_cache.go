package cli

type commandCache struct {
	clear commandCacheClear
	info  commandCacheInfo
	set   commandCacheSetParams
	sync  commandCacheSync
}

func (c *commandCache) setup(app appServices, parent commandParent) {
	cmd := parent.Command("cache", "Commands to manipulate local cache").Hidden()

	c.clear.setup(app, cmd)
	c.info.setup(app, cmd)
	c.set.setup(app, cmd)
	c.sync.setup(app, cmd)
}
