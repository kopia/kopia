package content

import (
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
)

// RecoverIndexFromPackBlob attempts to recover index blob entries from a given pack file.
// Pack file length may be provided (if known) to reduce the number of bytes that are read from the storage.
func (bm *WriteManager) RecoverIndexFromPackBlob(ctx context.Context, packFile blob.ID, packFileLength int64, commit bool) ([]Info, error) {
	var localIndexBytes gather.WriteBuffer
	defer localIndexBytes.Close()

	if err := bm.readPackFileLocalIndex(ctx, packFile, packFileLength, &localIndexBytes); err != nil {
		return nil, err
	}

	ndx, err := index.Open(localIndexBytes.Bytes().ToByteSlice(), nil, bm.format.Encryptor().Overhead)
	if err != nil {
		return nil, errors.Errorf("unable to open index in file %v", packFile)
	}

	var recovered []Info

	err = ndx.Iterate(index.AllIDs, func(is index.Info) error {
		recovered = append(recovered, is)

		return nil
	})

	if commit {
		bm.lock()
		defer bm.unlock(ctx)

		for _, is := range recovered {
			bm.packIndexBuilder.Add(is)
		}
	}

	return recovered, errors.Wrap(err, "error iterating index entries")
}

type packContentPostamble struct {
	localIndexIV     []byte
	localIndexOffset uint32
	localIndexLength uint32
}

func (p *packContentPostamble) toBytes() ([]byte, error) {
	// 4 varints + IV + 4 bytes of checksum + 1 byte of postamble length
	n := 0
	buf := make([]byte, 4*binary.MaxVarintLen64+len(p.localIndexIV)+4+1)

	n += binary.PutUvarint(buf[n:], uint64(1))                   // version flag
	n += binary.PutUvarint(buf[n:], uint64(len(p.localIndexIV))) // length of local index IV
	copy(buf[n:], p.localIndexIV)
	n += len(p.localIndexIV)
	n += binary.PutUvarint(buf[n:], uint64(p.localIndexOffset))
	n += binary.PutUvarint(buf[n:], uint64(p.localIndexLength))

	checksum := crc32.ChecksumIEEE(buf[0:n])
	binary.BigEndian.PutUint32(buf[n:], checksum)
	n += 4

	if n > 255 { //nolint:mnd
		return nil, errors.Errorf("postamble too long: %v", n)
	}

	buf[n] = byte(n)

	return buf[0 : n+1], nil
}

// findPostamble detects if a given content of bytes contains a possibly valid postamble, and returns it if so
// NOTE, even if this function returns a postamble, it should not be trusted to be correct, since it's not
// cryptographically signed. this is to facilitate data recovery.
func findPostamble(b []byte) *packContentPostamble {
	if len(b) == 0 {
		// no postamble
		return nil
	}

	// length of postamble is the last byte
	postambleLength := int(b[len(b)-1])
	if postambleLength < 5 { //nolint:mnd
		// too short, must be at least 5 bytes (checksum + own length)
		return nil
	}

	postambleStart := len(b) - 1 - postambleLength
	postambleEnd := len(b) - 1

	if postambleStart < 0 {
		// invalid last byte
		return nil
	}

	postambleBytes := b[postambleStart:postambleEnd]
	payload, checksumBytes := postambleBytes[0:len(postambleBytes)-4], postambleBytes[len(postambleBytes)-4:]
	checksum := binary.BigEndian.Uint32(checksumBytes)
	validChecksum := crc32.ChecksumIEEE(payload)

	if checksum != validChecksum {
		// invalid checksum, not a valid postamble
		return nil
	}

	return decodePostamble(payload)
}

func decodePostamble(payload []byte) *packContentPostamble {
	flags, n := binary.Uvarint(payload)
	if n <= 0 {
		// invalid flags
		return nil
	}

	if flags != 1 {
		// unsupported flag
		return nil
	}

	payload = payload[n:]

	ivLength, n := binary.Uvarint(payload)
	if n <= 0 {
		// invalid flags
		return nil
	}

	payload = payload[n:]
	if ivLength > uint64(len(payload)) {
		// invalid IV length
		return nil
	}

	iv := payload[0:ivLength]
	payload = payload[ivLength:]

	off, n := binary.Uvarint(payload)
	if n <= 0 {
		// invalid offset
		return nil
	}

	payload = payload[n:]

	length, n := binary.Uvarint(payload)
	if n <= 0 {
		// invalid offset
		return nil
	}

	return &packContentPostamble{
		localIndexIV:     iv,
		localIndexLength: uint32(length), //nolint:gosec
		localIndexOffset: uint32(off),    //nolint:gosec
	}
}

func (sm *SharedManager) buildLocalIndex(mp format.MutableParameters, pending index.Builder, output *gather.WriteBuffer) error {
	if err := pending.Build(output, mp.IndexVersion); err != nil {
		return errors.Wrap(err, "unable to build local index")
	}

	return nil
}

// appendPackFileIndexRecoveryData appends data designed to help with recovery of pack index in case it gets damaged or lost.
func (sm *SharedManager) appendPackFileIndexRecoveryData(mp format.MutableParameters, pending index.Builder, output *gather.WriteBuffer) error {
	// build, encrypt and append local index
	localIndexOffset := output.Length()

	var localIndex gather.WriteBuffer
	defer localIndex.Close()

	if err := sm.buildLocalIndex(mp, pending, &localIndex); err != nil {
		return err
	}

	localIndexIV := sm.hashData(nil, localIndex.Bytes())

	var encryptedLocalIndex gather.WriteBuffer
	defer encryptedLocalIndex.Close()

	if err := sm.format.Encryptor().Encrypt(localIndex.Bytes(), localIndexIV, &encryptedLocalIndex); err != nil {
		return errors.Wrap(err, "encryption error")
	}

	postamble := packContentPostamble{
		localIndexIV:     localIndexIV,
		localIndexOffset: uint32(localIndexOffset),             //nolint:gosec
		localIndexLength: uint32(encryptedLocalIndex.Length()), //nolint:gosec
	}

	if _, err := encryptedLocalIndex.Bytes().WriteTo(output); err != nil {
		return errors.Wrap(err, "error copying encrypted index to buffer")
	}

	postambleBytes, err := postamble.toBytes()
	if err != nil {
		return err
	}

	output.Append(postambleBytes)

	return nil
}
