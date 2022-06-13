package cli

type commandRepositoryThrottle struct {
	get commandRepositoryThrottleGet
	set commandRepositoryThrottleSet
}

func (c *commandRepositoryThrottle) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("throttle", "Commands to manipulate throttle configuration")

	c.get.setup(svc, cmd)
	c.set.setup(svc, cmd)
}
