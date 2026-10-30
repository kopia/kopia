package cli

type commandServerThrottle struct {
	get commandServerThrottleGet
	set commandServerThrottleSet
}

func (c *commandServerThrottle) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("throttle", "Control throttling parameters for a running server")
	c.get.setup(svc, cmd)
	c.set.setup(svc, cmd)
}
