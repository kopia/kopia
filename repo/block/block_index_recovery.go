package block

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"reflect"

	"github.com/pkg/errors"
)

// RecoverIndexFromPackFile attempts to recover index block entries from a given pack file.
// Pack file length may be provided (if known) to reduce the number of bytes that are read from the storage.
func (bm *Manager) RecoverIndexFromPackFile(ctx context.Context, packFile string, packFileLength int64, commit bool) ([]Info, error) {
	localIndexBytes, err := bm.readPackFileLocalIndex(ctx, packFile, packFileLength)
	if err != nil {
		return nil, err
	}

	ndx, err := openPackIndex(bytes.NewReader(localIndexBytes))
	if err != nil {
		return nil, errors.Errorf("unable to open index in file %v", packFile)
	}
	defer ndx.Close() //nolint:errcheck

	var recovered []Info

	err = ndx.Iterate("", func(i Info) error {
		recovered = append(recovered, i)
		if commit {
			bm.packIndexBuilder.Add(i)
		}
		return nil
	})

	return recovered, err
}

type packBlockPostamble struct {
	localIndexIV     []byte
	localIndexOffset uint32
	localIndexLength uint32
}

func (p *packBlockPostamble) toBytes() ([]byte, error) {
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
	if n > 255 {
		return nil, errors.Errorf("postamble too long: %v", n)
	}

	buf[n] = byte(n)
	return buf[0 : n+1], nil
}

// findPostamble detects if a given block of bytes contains a possibly valid postamble, and returns it if so
// NOTE, even if this function returns a postamble, it should not be trusted to be correct, since it's not
// cryptographically signed. this is to facilitate data recovery.
func findPostamble(b []byte) *packBlockPostamble {
	if len(b) == 0 {
		// no postamble
		return nil
	}

	// length of postamble is the last byte
	postambleLength := int(b[len(b)-1])
	if postambleLength < 5 {
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

func decodePostamble(payload []byte) *packBlockPostamble {
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

	return &packBlockPostamble{
		localIndexIV:     iv,
		localIndexLength: uint32(length),
		localIndexOffset: uint32(off),
	}
}

func (bm *Manager) buildLocalIndex(pending packIndexBuilder) ([]byte, error) {
	var buf bytes.Buffer
	if err := pending.Build(&buf); err != nil {
		return nil, errors.Wrap(err, "unable to build local index")
	}

	return buf.Bytes(), nil
}

// appendPackFileIndexRecoveryData appends data designed to help with recovery of pack index in case it gets damaged or lost.
func (bm *Manager) appendPackFileIndexRecoveryData(blockData []byte, pending packIndexBuilder) ([]byte, error) {
	// build, encrypt and append local index
	localIndexOffset := len(blockData)
	localIndex, err := bm.buildLocalIndex(pending)
	if err != nil {
		return nil, err
	}

	localIndexIV := bm.hashData(localIndex)
	encryptedLocalIndex, err := bm.encryptor.Encrypt(localIndex, localIndexIV)
	if err != nil {
		return nil, err
	}

	postamble := packBlockPostamble{
		localIndexIV:     localIndexIV,
		localIndexOffset: uint32(localIndexOffset),
		localIndexLength: uint32(len(encryptedLocalIndex)),
	}

	blockData = append(blockData, encryptedLocalIndex...)
	postambleBytes, err := postamble.toBytes()
	if err != nil {
		return nil, err
	}

	blockData = append(blockData, postambleBytes...)

	pa2 := findPostamble(blockData)
	if pa2 == nil {
		log.Fatalf("invalid postamble written, that could not be immediately decoded, it's a bug")
	}

	if !reflect.DeepEqual(postamble, *pa2) {
		log.Fatalf("postamble did not round-trip: %v %v", postamble, *pa2)
	}

	return blockData, nil
}

func (bm *Manager) readPackFileLocalIndex(ctx context.Context, packFile string, packFileLength int64) ([]byte, error) {
	payload, err := bm.st.GetBlock(ctx, packFile, 0, -1)
	if err != nil {
		return nil, err
	}

	postamble := findPostamble(payload)
	if postamble == nil {
		return nil, errors.Errorf("unable to find valid postamble in file %v", packFile)
	}

	if uint64(postamble.localIndexOffset+postamble.localIndexLength) > uint64(len(payload)) {
		// invalid offset/length
		return nil, errors.Errorf("unable to find valid local index in file %v", packFile)
	}

	encryptedLocalIndexBytes := payload[postamble.localIndexOffset : postamble.localIndexOffset+postamble.localIndexLength]
	if encryptedLocalIndexBytes == nil {
		return nil, errors.Errorf("unable to find valid local index in file %v", packFile)
	}

	localIndexBytes, err := bm.decryptAndVerify(encryptedLocalIndexBytes, postamble.localIndexIV)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt local index")
	}

	return localIndexBytes, nil
}
