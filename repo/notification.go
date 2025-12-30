// Package notification provides a mechanism to send notifications for various events.
package repo

import (
	"context"

	"github.com/kopia/kopia/internal/grpcapi"
)

// RemoteNotifications is an interface implemented by repository clients that support remote notifications.
type RemoteNotifications interface {
	SendNotification(ctx context.Context, templateName string, templateDataJSON []byte, templateDataType grpcapi.NotificationEventArgType, severity int32) error
}
