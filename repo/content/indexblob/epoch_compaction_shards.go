package indexblob

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

var errCompactionTemporaryDirectoryMissing = errors.New("index compaction temporary directory disappeared")

func compactionTemporaryFileError(err error) error {
	if stderrors.Is(err, os.ErrNotExist) {
		return stderrors.Join(errCompactionTemporaryDirectoryMissing, err)
	}

	return err
}

// compactionShardStore retains a single small output in memory. Larger output
// sets are spooled to one file, so neither output buffers nor open files grow
// with the number of shards. Nothing is published until every shard is built.
type compactionShardStore struct {
	tempDirectory string
	first         []byte
	file          *os.File
	path          string
	count         int
	maxLength     uint64
}

func (s *compactionShardStore) add(ctx context.Context, data gather.Bytes) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "staging index compaction shard canceled")
	}

	if s.count == 0 {
		s.first = make([]byte, data.Length())
		if _, err := io.ReadFull(data.Reader(), s.first); err != nil {
			return errors.Wrap(err, "buffering first index compaction shard")
		}
	} else {
		if err := s.ensureFile(); err != nil {
			return err
		}

		if err := s.write(data); err != nil {
			return err
		}
	}

	s.maxLength = max(s.maxLength, compactionShardLength(data))
	s.count++

	return nil
}

func (s *compactionShardStore) ensureFile() error {
	if s.file != nil {
		return nil
	}

	f, err := os.CreateTemp(s.tempDirectory, "kopia-compaction-shards-*")
	if err != nil {
		return errors.Wrap(compactionTemporaryFileError(err), "creating index compaction shard spool")
	}

	s.file = f

	s.path = f.Name()
	if err := s.write(gather.FromSlice(s.first)); err != nil {
		return err
	}

	s.first = nil

	return nil
}

func compactionShardLength(data gather.Bytes) uint64 {
	var length uint64
	for _, part := range data.Slices {
		length += uint64(len(part))
	}

	return length
}

func (s *compactionShardStore) write(data gather.Bytes) error {
	var header [12]byte

	binary.LittleEndian.PutUint64(header[:8], compactionShardLength(data))

	h := crc32.NewIEEE()
	for _, b := range data.Slices {
		_, _ = h.Write(b)
	}

	binary.LittleEndian.PutUint32(header[8:], h.Sum32())

	if _, err := s.file.Write(header[:]); err != nil {
		return errors.Wrap(err, "writing temporary index shard header")
	}

	_, err := io.Copy(s.file, data.Reader())

	return errors.Wrap(err, "writing temporary index shard data")
}

func (s *compactionShardStore) replay(ctx context.Context, emit func(gather.Bytes) error) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "replaying index compaction shards canceled")
	}

	if s.file == nil {
		if s.count == 0 {
			return nil
		}

		return emit(gather.FromSlice(s.first))
	}

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewinding temporary index shards")
	}

	var data []byte

	for shardNumber := range s.count {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "replaying index compaction shards canceled")
		}

		var err error

		data, err = s.readShard(data)
		if err != nil {
			return err
		}

		// Validate the end before emitting the final shard: publishing it can
		// make this output set visible to other clients.
		if shardNumber == s.count-1 {
			if err := s.checkEnd(); err != nil {
				return err
			}
		}

		if err := emit(gather.FromSlice(data)); err != nil {
			return err
		}
	}

	return nil
}

func (s *compactionShardStore) readShard(data []byte) ([]byte, error) {
	var header [12]byte
	if _, err := io.ReadFull(s.file, header[:]); err != nil {
		return nil, errors.Wrap(err, "reading temporary index shard header")
	}

	n := binary.LittleEndian.Uint64(header[:8])
	if n == 0 || n > s.maxLength || n > math.MaxInt {
		return nil, errors.New("invalid temporary index shard length")
	}

	if cap(data) < int(n) {
		data = make([]byte, int(n))
	} else {
		data = data[:int(n)]
	}

	if _, err := io.ReadFull(s.file, data); err != nil {
		return nil, errors.Wrap(err, "reading temporary index shard data")
	}

	if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[8:]) {
		return nil, errors.New("temporary index shard checksum mismatch")
	}

	return data, nil
}

func (s *compactionShardStore) checkEnd() error {
	var extra [1]byte

	n, err := io.ReadFull(s.file, extra[:])
	if n != 0 {
		return errors.New("unexpected trailing temporary index shard data")
	}

	if err != io.EOF {
		return errors.Wrap(err, "reading temporary index shard trailer")
	}

	return nil
}

func (s *compactionShardStore) close() error {
	s.first = nil

	var closeErr error
	if s.file != nil {
		closeErr = errors.Wrap(s.file.Close(), "closing temporary index shards")
		s.file = nil
	}

	if s.path == "" {
		return closeErr
	}

	removeErr := os.Remove(s.path)
	if removeErr == nil || stderrors.Is(removeErr, os.ErrNotExist) {
		s.path = ""
		removeErr = nil
	}

	return stderrors.Join(closeErr, errors.Wrap(removeErr, "removing temporary index shards"))
}
