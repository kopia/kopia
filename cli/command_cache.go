package cli

type commandCache struct {
	clear commandCacheClear
	info  commandCacheInfo
	set   commandCacheSetParams
	sync  commandCacheSync
}

func (c *commandCache) setup(parent commandParent) {
	cmd := parent.Command("cache", "Commands to manipulate local cache").Hidden()

	c.clear.setup(cmd)
	c.info.setup(cmd)
	c.set.setup(cmd)
	c.sync.setup(cmd)
}
