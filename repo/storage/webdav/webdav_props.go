package webdav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var blockInfoRequest = []byte(`<d:propfind xmlns:d='DAV:'>
<d:prop>
  <d:displayname/>
  <d:resourcetype/>
  <d:getcontentlength/>
  <d:getlastmodified/>
</d:prop>
</d:propfind>`)

func (d *davStorage) propFindRequest(urlStr string, depth string) (*http.Request, error) {
	req, err := http.NewRequest("PROPFIND", urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "text/xml;charset=UTF-8")
	req.Header.Add("Accept", "application/xml,text/xml")
	req.Header.Add("Accept-Charset", "utf-8")
	req.Header.Add("Accept-Encoding", "")
	req.Header.Add("Depth", depth)
	return req, nil
}

type props struct {
	Status   string   `xml:"DAV: status"`
	Name     string   `xml:"DAV: prop>displayname,omitempty"`
	Type     xml.Name `xml:"DAV: prop>resourcetype>collection,omitempty"`
	Size     string   `xml:"DAV: prop>getcontentlength,omitempty"`
	Modified string   `xml:"DAV: prop>getlastmodified,omitempty"`
}

type response struct {
	Href  string  `xml:"DAV: href"`
	Props []props `xml:"DAV: propstat"`
}

type multiResponse struct {
	Responses []response `xml:"DAV: response"`
}

type webdavDirEntry struct {
	name         string
	length       int64
	modTime      time.Time
	isCollection bool
}

func (d *davStorage) propFindChildren(urlStr string) ([]webdavDirEntry, error) {
	req, err := d.propFindRequest(urlStr, "1")
	if err != nil {
		return nil, fmt.Errorf("can't create PROPFIND request: %v", err)
	}

	resp, err := d.executeRequest(req, blockInfoRequest)
	if err != nil {
		return nil, fmt.Errorf("unable to execute webdav request: %v", err)
	}

	defer resp.Body.Close() // nolint:errcheck

	var ms multiResponse
	dec := xml.NewDecoder(resp.Body)

	if err := dec.Decode(&ms); err != nil {
		return nil, fmt.Errorf("unable to decode webdav response: %v", err)
	}

	var entries []webdavDirEntry

	for _, r := range ms.Responses {
		var e webdavDirEntry

		for _, p := range r.Props {
			if !strings.Contains(p.Status, " 200 ") {
				continue
			}

			e.name = p.Name
			e.length, _ = strconv.ParseInt(p.Size, 10, 64)
			e.modTime, _ = time.Parse(time.RFC1123, p.Modified)
			e.isCollection = p.Type.Local == "collection"
		}

		entries = append(entries, e)
	}

	return entries, nil
}
