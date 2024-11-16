package acl

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
)

// AccessLevel specifies access level.
//
//nolint:recvcheck
type AccessLevel int

// accessLevelToString maps supported access levels to strings.
//
//nolint:gochecknoglobals
var accessLevelToString = map[AccessLevel]string{
	AccessLevelNone:   "NONE",
	AccessLevelRead:   "READ",
	AccessLevelAppend: "APPEND",
	AccessLevelFull:   "FULL",
}

// stringToAccessLevel maps strings to supported access levels.
//
//nolint:gochecknoglobals
var stringToAccessLevel = map[string]AccessLevel{}

func init() {
	for k, v := range accessLevelToString {
		stringToAccessLevel[v] = k
	}
}

func (a AccessLevel) String() string {
	s, ok := accessLevelToString[a]
	if !ok {
		return strconv.Itoa(int(a))
	}

	return s
}

// MarshalJSON implements json.Marshaler.
func (a AccessLevel) MarshalJSON() ([]byte, error) {
	j, ok := accessLevelToString[a]
	if !ok {
		return nil, errors.Errorf("Invalid access level: %v", a)
	}

	//nolint:wrapcheck
	return json.Marshal(j)
}

// UnmarshalJSON implements json.Unmarshaler.
func (a *AccessLevel) UnmarshalJSON(b []byte) error {
	var s string

	if err := json.Unmarshal(b, &s); err != nil {
		return errors.Wrap(err, "error unmarshaling access level")
	}

	*a = stringToAccessLevel[s]

	return nil
}

var (
	_ json.Marshaler   = AccessLevelNone
	_ json.Unmarshaler = (*AccessLevel)(nil)
)

// Supported access levels.
const (
	AccessLevelNone   AccessLevel = 1 // no access
	AccessLevelRead   AccessLevel = 2 // permissions to view, but not change
	AccessLevelAppend AccessLevel = 3 // permissions to view/add but not update/delete.
	AccessLevelFull   AccessLevel = 4 // permission to view/add/update/delete.
)

// SupportedAccessLevels returns the list of supported access levels.
func SupportedAccessLevels() []string {
	return []string{"NONE", "READ", "APPEND", "FULL"}
}

// ParseAccessLevel parses the provided string into an AccessLevel.
func ParseAccessLevel(s string) (AccessLevel, error) {
	al, ok := stringToAccessLevel[s]
	if ok {
		return al, nil
	}

	return 0, errors.Errorf("access level '%v' is unsupported", al)
}
