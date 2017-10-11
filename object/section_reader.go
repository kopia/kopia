package object

import (
	"fmt"
	"io"
)

type objectSectionReader struct {
	baseReader      ObjectReader
	start, length   int64
	currentPosition int64
}

func (osr *objectSectionReader) Close() error {
	return osr.baseReader.Close()
}

func (osr *objectSectionReader) Length() int64 {
	return osr.length
}

func (osr *objectSectionReader) Read(p []byte) (n int, err error) {
	if osr.currentPosition >= osr.length {
		return 0, io.EOF
	}
	if max := osr.length - osr.currentPosition; int64(len(p)) > max {
		p = p[0:max]
	}
	n, err = osr.baseReader.Read(p)
	osr.currentPosition += int64(n)
	return
}

func (osr *objectSectionReader) Seek(offset int64, whence int) (int64, error) {
	if whence == 1 {
		return osr.Seek(osr.currentPosition+offset, 0)
	}

	if whence == 2 {
		return osr.Seek(osr.length+offset, 0)
	}

	if offset < 0 {
		return -1, fmt.Errorf("invalid seek %v %v", offset, whence)
	}

	if offset > osr.length {
		offset = osr.length
	}

	osr.currentPosition = offset

	_, err := osr.baseReader.Seek(osr.start+osr.currentPosition, 0)
	return osr.currentPosition, err
}

func newObjectSectionReader(start, length int64, baseReader ObjectReader) (ObjectReader, error) {
	r := &objectSectionReader{
		baseReader: baseReader,
		start:      start,
		length:     length,
	}

	if _, err := r.Seek(0, 0); err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}
