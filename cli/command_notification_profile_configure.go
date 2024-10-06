package cli

type commandNotificationProfileConfigure struct {
	commandNotificationConfigureEmail
	commandNotificationConfigurePushover
	commandNotificationConfigureWebhook
	commandNotificationConfigureTestSender
}

func (c *commandNotificationProfileConfigure) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("configure", "Setup notifications").Alias("setup")
	c.commandNotificationConfigureEmail.setup(svc, cmd)
	c.commandNotificationConfigurePushover.setup(svc, cmd)
	c.commandNotificationConfigureWebhook.setup(svc, cmd)

	if svc.enableTestOnlyFlags() {
		c.commandNotificationConfigureTestSender.setup(svc, cmd)
	}
}
