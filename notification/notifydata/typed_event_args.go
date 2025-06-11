package notifydata

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/grpcapi"
)

// TypedEventArgs is an interface that represents the event arguments for notifications.
type TypedEventArgs interface {
	// EventArgsType returns the type of event arguments.
	EventArgsType() grpcapi.NotificationEventArgType
}

// UnmarshalEventArgs unmarshals the provided JSON data into a TypedEventArgs based on the specified notificationEventArgType.
func UnmarshalEventArgs(data []byte, notificationEventArgType grpcapi.NotificationEventArgType) (TypedEventArgs, error) {
	var payload TypedEventArgs

	switch notificationEventArgType {
	case grpcapi.NotificationEventArgType_ARG_TYPE_EMPTY:
		payload = &EmptyEventData{}

	case grpcapi.NotificationEventArgType_ARG_TYPE_MULTI_SNAPSHOT_STATUS:
		payload = &MultiSnapshotStatus{}

	case grpcapi.NotificationEventArgType_ARG_TYPE_ERROR_INFO:
		payload = &ErrorInfo{}

	default:
		return nil, errors.Errorf("unsupported notification event arg type: %v", notificationEventArgType)
	}

	if err := json.Unmarshal(data, payload); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal event args of type %v", notificationEventArgType)
	}

	return payload, nil
}
