package acl

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
)

// AccessLevel specifies access level.
type AccessLevel int

// AccessLevelToString maps supported access levels to strings.
var AccessLevelToString = map[AccessLevel]string{
	AccessLevelNone:   "NONE",
	AccessLevelRead:   "READ",
	AccessLevelAppend: "APPEND",
	AccessLevelFull:   "FULL",
}

// StringToAccessLevel maps strings to supported access levels.
var StringToAccessLevel = map[string]AccessLevel{}

func init() {
	for k, v := range AccessLevelToString {
		StringToAccessLevel[v] = k
	}
}

func (a AccessLevel) String() string {
	s, ok := AccessLevelToString[a]
	if !ok {
		return strconv.Itoa(int(a))
	}

	return s
}

// MarshalJSON implements json.Marshaler.
func (a AccessLevel) MarshalJSON() ([]byte, error) {
	j, ok := AccessLevelToString[a]
	if !ok {
		return nil, errors.Errorf("Invalid access level: %v", a)
	}

	return json.Marshal(j)
}

// UnmarshalJSON implements json.Unmarshaler.
func (a *AccessLevel) UnmarshalJSON(b []byte) error {
	var s string

	if err := json.Unmarshal(b, &s); err != nil {
		return errors.Wrap(err, "error unmarshaling access level")
	}

	*a = StringToAccessLevel[s]

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
