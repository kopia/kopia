package fs

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"
)

const modeChars = "dalTLDpSugct"

type jsonDirectoryEntry struct {
	Name     string    `json:"name"`
	Mode     string    `json:"mode,omitempty"`
	Size     string    `json:"size,omitempty"`
	Time     time.Time `json:"modTime"`
	Owner    string    `json:"owner,omitempty"`
	ObjectID string    `json:"oid,omitempty"`
}

func (de *Entry) fromJSON(jde *jsonDirectoryEntry) error {
	de.Name = jde.Name

	if mode, err := parseFileModeAndPermissions(jde.Mode); err == nil {
		de.FileMode = mode
	} else {
		return fmt.Errorf("invalid mode: %v", err)
	}

	de.ModTime = jde.Time

	if jde.Owner != "" {
		if c, err := fmt.Sscanf(jde.Owner, "%d:%d", &de.OwnerID, &de.GroupID); err != nil || c != 2 {
			return fmt.Errorf("invalid owner: %v", err)
		}
	}
	de.ObjectID = repo.ObjectID(jde.ObjectID)

	if jde.Size != "" {
		if s, err := strconv.ParseInt(jde.Size, 10, 64); err == nil {
			de.FileSize = s
		} else {
			return fmt.Errorf("invalid size: %v", err)
		}
	}
	return nil
}

// parseFileModeAndPermissions converts file mode string to os.FileMode
func parseFileModeAndPermissions(s string) (os.FileMode, error) {
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return parseFilePermissions(s)
	}

	var mode os.FileMode

	if m, err := parseFileMode(s[0:colon]); err == nil {
		mode |= m
	} else {
		return 0, err
	}

	if m, err := parseFilePermissions(s[colon+1:]); err == nil {
		mode |= m
	} else {
		return 0, err
	}

	return mode, nil
}

func parseFileMode(s string) (os.FileMode, error) {
	var mode os.FileMode
	for _, c := range s {
		switch c {
		case 'd':
			mode |= os.ModeDir
		case 'a':
			mode |= os.ModeAppend
		case 'l':
			mode |= os.ModeExclusive
		case 'T':
			mode |= os.ModeTemporary
		case 'L':
			mode |= os.ModeSymlink
		case 'D':
			mode |= os.ModeDevice
		case 'p':
			mode |= os.ModeNamedPipe
		case 'S':
			mode |= os.ModeSocket
		case 'u':
			mode |= os.ModeSetuid
		case 'g':
			mode |= os.ModeSetgid
		case 'c':
			mode |= os.ModeCharDevice
		case 't':
			mode |= os.ModeSticky
		default:
			return 0, fmt.Errorf("unsupported mode: '%v'", c)
		}
	}
	return mode, nil
}

func parseFilePermissions(perm string) (os.FileMode, error) {
	s, err := strconv.ParseUint(perm, 8, 32)
	if err != nil {
		return 0, err
	}
	return os.FileMode(s), nil
}

type directoryWriter struct {
	io.Closer

	writer    io.Writer
	buf       []byte
	separator []byte

	lastNameWritten string
}

func (dw *directoryWriter) WriteEntry(e *Entry) error {
	if dw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, dw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", dw.lastNameWritten, e.Name)
		}
		dw.lastNameWritten = e.Name
	}

	jde := jsonDirectoryEntry{
		Name:     e.Name,
		Mode:     formatModeAndPermissions(e.FileMode),
		Time:     e.ModTime.UTC(),
		Owner:    fmt.Sprintf("%d:%d", e.OwnerID, e.GroupID),
		ObjectID: string(e.ObjectID),
	}

	if e.FileMode.IsRegular() {
		jde.Size = strconv.FormatInt(e.FileSize, 10)
	}

	v, _ := json.Marshal(&jde)

	dw.writer.Write(dw.separator)
	dw.writer.Write(v)
	dw.separator = []byte(",")

	return nil
}

func formatModeAndPermissions(m os.FileMode) string {
	const str = "dalTLDpSugct"
	var buf [32]byte
	w := 0
	for i, c := range str {
		if m&(1<<uint(32-1-i)) != 0 {
			buf[w] = byte(c)
			w++
		}
	}
	if w > 0 {
		buf[w] = ':'
		w++
	}

	return string(buf[:w]) + strconv.FormatInt(int64(m&os.ModePerm), 8)
}

func (dw *directoryWriter) Close() error {
	dw.writer.Write([]byte("]}"))
	return nil
}

func (*directoryWriter) serializeLengthPrefixedString(buf []byte, s string) int {
	offset := binary.PutUvarint(buf, uint64(len(s)))
	copy(buf[offset:], s)
	offset += len(s)
	return offset
}

func newDirectoryWriter(w io.Writer) *directoryWriter {
	dw := &directoryWriter{
		writer: w,
	}

	var f directoryFormat
	f.Version = 1

	io.WriteString(w, "{\"format\":")
	b, _ := json.Marshal(&f)
	w.Write(b)
	io.WriteString(w, ",\"entries\":[")
	dw.separator = []byte("")

	return dw
}

type directoryReader struct {
	reader  io.Reader
	decoder *json.Decoder
}

func (dr *directoryReader) ReadNext() (*Entry, error) {
	if dr.decoder.More() {
		var jde jsonDirectoryEntry
		if err := dr.decoder.Decode(&jde); err != nil {
			return nil, err
		}

		var de Entry
		if err := de.fromJSON(&jde); err != nil {
			return nil, err
		}

		return &de, nil
	}

	if err := ensureDelimiter(dr.decoder, json.Delim(']')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('}')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	return nil, io.EOF
}

func invalidDirectoryError(cause error) error {
	return fmt.Errorf("invalid directory data: %v", cause)
}

type directoryFormat struct {
	Version int `json:"version"`
}

func ensureDelimiter(d *json.Decoder, expected json.Delim) error {
	t, err := d.Token()
	if err != nil {
		return err
	}

	if t != expected {
		return fmt.Errorf("expected '%v', got %v", expected.String(), t)
	}

	return nil
}
func ensureStringToken(d *json.Decoder, expected string) error {
	t, err := d.Token()
	if err != nil {
		return err
	}

	if s, ok := t.(string); ok {
		if s == expected {
			return nil
		}
	}

	return fmt.Errorf("expected '%v', got '%v'", expected, t)
}

func newDirectoryReader(r io.Reader) (*directoryReader, error) {
	dr := &directoryReader{
		decoder: json.NewDecoder(r),
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('{')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if err := ensureStringToken(dr.decoder, "format"); err != nil {
		return nil, invalidDirectoryError(err)
	}

	// Parse format and trailing comma
	var format directoryFormat
	if err := dr.decoder.Decode(&format); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if format.Version != 1 {
		return nil, invalidDirectoryError(fmt.Errorf("unsupported version: %v", format.Version))
	}

	if err := ensureStringToken(dr.decoder, "entries"); err != nil {
		return nil, invalidDirectoryError(err)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('[')); err != nil {
		return nil, invalidDirectoryError(err)
	}

	return dr, nil
}

// ReadDirectory loads the serialized Directory from the specified Reader.
func ReadDirectory(r io.Reader, namePrefix string) (Directory, error) {
	dr, err := newDirectoryReader(r)
	if err != nil {
		return nil, err
	}

	var dir Directory
	for {
		e, err := dr.ReadNext()
		if e != nil {
			dir = append(dir, e)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return dir, nil
}
