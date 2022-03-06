package cli

type commandCache struct {
	clear    commandCacheClear
	info     commandCacheInfo
	prefetch commandCachePrefetch
	set      commandCacheSetParams
	sync     commandCacheSync
}

func (c *commandCache) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("cache", "Commands to manipulate local cache").Hidden()

	c.clear.setup(svc, cmd)
	c.info.setup(svc, cmd)
	c.prefetch.setup(svc, cmd)
	c.set.setup(svc, cmd)
	c.sync.setup(svc, cmd)
}
