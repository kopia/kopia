package cli

type commandNotification struct {
	profile  commandNotificationProfile
	template commandNotificationTemplate
}

func (c *commandNotification) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("notification", "Notifications").Alias("notifications")

	c.profile.setup(svc, cmd)
	c.template.setup(svc, cmd)
}
