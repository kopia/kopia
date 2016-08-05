package fs

import (
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
	Name       string               `json:"name"`
	Mode       string               `json:"mode,omitempty"`
	Size       string               `json:"size,omitempty"`
	Time       time.Time            `json:"modTime"`
	Owner      string               `json:"owner,omitempty"`
	ObjectID   string               `json:"oid,omitempty"`
	SubEntries []jsonDirectoryEntry `json:"entries,omitempty"`
}

func (de *EntryMetadata) fromJSON(jde *jsonDirectoryEntry) error {
	if jde.Name == "" {
		return fmt.Errorf("empty entry name")
	}
	de.Name = jde.Name

	if jde.Mode != "" {
		if mode, err := parseFileModeAndPermissions(jde.Mode); err == nil {
			de.FileMode = mode
		} else {
			return fmt.Errorf("invalid mode or permissions: '%v'", jde.Mode)
		}
	} else {
		de.FileMode = 0777
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

func (dw *directoryWriter) WriteEntry(e *EntryMetadata, children []*EntryMetadata) error {
	if dw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, dw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", dw.lastNameWritten, e.Name)
		}
		dw.lastNameWritten = e.Name
	}

	jde := toJSONEntry(e)

	if len(children) > 0 {
		jde.SubEntries = make([]jsonDirectoryEntry, len(children))
		for i, se := range children {
			jde.SubEntries[i] = toJSONEntry(se)
		}
	}

	v, _ := json.Marshal(&jde)

	dw.writer.Write(dw.separator)
	dw.writer.Write(v)
	dw.separator = []byte(",")

	return nil
}

func toJSONEntry(e *EntryMetadata) jsonDirectoryEntry {
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

	return jde
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

func (dr *directoryReader) readNext(jde *jsonDirectoryEntry) error {
	if dr.decoder.More() {
		return dr.decoder.Decode(&jde)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim(']')); err != nil {
		return invalidDirectoryError(err)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('}')); err != nil {
		return invalidDirectoryError(err)
	}

	return io.EOF
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

func readDirectoryMetadataEntries(r io.Reader) ([]*EntryMetadata, error) {
	dr, err := newDirectoryReader(r)
	if err != nil {
		return nil, err
	}

	var entries []*EntryMetadata
	var bundles [][]*EntryMetadata

	for {
		var e jsonDirectoryEntry

		if err := dr.readNext(&e); err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var entryMetadata EntryMetadata
		if err := entryMetadata.fromJSON(&e); err != nil {
			return nil, err
		}

		if len(e.SubEntries) > 0 {
			bundle := make([]*EntryMetadata, 0, len(e.SubEntries))

			var currentOffset int64

			var bundleEntry EntryMetadata
			if err := bundleEntry.fromJSON(&e); err != nil {
				return nil, err
			}

			for _, s := range e.SubEntries {
				var subEntry EntryMetadata
				if err := subEntry.fromJSON(&s); err != nil {
					return nil, err
				}
				subEntry.ObjectID = repo.NewSectionObjectID(currentOffset, subEntry.FileSize, entryMetadata.ObjectID)
				currentOffset += subEntry.FileSize
				bundle = append(bundle, &subEntry)
			}

			if currentOffset != entryMetadata.FileSize {
				return nil, fmt.Errorf("inconsistent size of '%v': %v (got %v)", entryMetadata.Name, entryMetadata.FileSize, currentOffset)
			}

			bundles = append(bundles, bundle)
		} else {
			entries = append(entries, &entryMetadata)
		}
	}

	if len(bundles) > 0 {
		if entries != nil {
			bundles = append(bundles, entries)
		}

		entries = mergeSortN(bundles)
	}

	return entries, nil
}

func mergeSort2(b1, b2 []*EntryMetadata) []*EntryMetadata {
	combinedLength := len(b1) + len(b2)
	result := make([]*EntryMetadata, 0, combinedLength)

	for len(b1) > 0 && len(b2) > 0 {
		if isLess(b1[0].Name, b2[0].Name) {
			result = append(result, b1[0])
			b1 = b1[1:]
		} else {
			result = append(result, b2[0])
			b2 = b2[1:]
		}
	}

	result = append(result, b1...)
	result = append(result, b2...)

	return result
}

func mergeSortN(slices [][]*EntryMetadata) []*EntryMetadata {
	switch len(slices) {
	case 1:
		return slices[0]
	case 2:
		return mergeSort2(slices[0], slices[1])
	default:
		mid := len(slices) / 2
		return mergeSort2(
			mergeSortN(slices[:mid]),
			mergeSortN(slices[mid:]))
	}
}
