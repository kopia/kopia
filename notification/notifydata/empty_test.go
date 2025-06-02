package notifydata_test

import (
	"testing"

	"github.com/kopia/kopia/notification/notifydata"
)

func TestEmptyEventInfo(t *testing.T) {
	testRoundTrip(t, &notifydata.EmptyEventData{})
}
