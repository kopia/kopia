package notifydata

import "github.com/kopia/kopia/internal/grpcapi"

// EmptyEventData is a placeholder for events that do not carry any additional data.
type EmptyEventData struct{}

// EventArgsType returns the type of event arguments for EmptyEventData.
func (e EmptyEventData) EventArgsType() grpcapi.NotificationEventArgType {
	return grpcapi.NotificationEventArgType_ARG_TYPE_EMPTY
}
