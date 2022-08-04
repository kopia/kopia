package ecc

import (
	"encoding/binary"
	"github.com/klauspost/reedsolomon"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/pkg/errors"
	"hash/crc32"
)

const (
	RsCrc321pEccName  = "RS-CRC32-1%"
	RsCrc322pEccName  = "RS-CRC32-2%"
	RsCrc325pEccName  = "RS-CRC32-5%"
	RsCrc3210pEccName = "RS-CRC32-10%"
)

// RsCrcEcc implements Reed-Solomon error codes with CRC32 error detection
type RsCrcEcc struct {
	Options
	DataShards   int
	ParityShards int
	ShardSize    int
	Enc          reedsolomon.Encoder
}

func NewRsCrcEcc(opts *Options, spaceUsedPercentage float32, shardSize int) (*RsCrcEcc, error) {
	result := new(RsCrcEcc)

	result.Options = *opts
	result.DataShards, result.ParityShards = ComputeShards(spaceUsedPercentage)
	result.ShardSize = shardSize

	var err error
	result.Enc, err = reedsolomon.New(result.DataShards, result.ParityShards,
		reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, errors.Wrap(err, "Error creating reedsolomon encoder")
	}

	return result, nil
}

func (r *RsCrcEcc) ComputeSizesFromOriginal(length int) (blocks, crcSize, shardSize, originalSize int) {
	crcSize = 4
	shardSize = r.ShardSize
	blocks = CeilInt(length, r.DataShards*shardSize)
	originalSize = length
	return
}

func (r *RsCrcEcc) ComputeSizesFromStored(length int) (blocks, crcSize, shardSize, originalSize int) {
	crcSize = 4
	shardSize = r.ShardSize
	blocks = CeilInt(length, (r.DataShards+r.ParityShards)*(crcSize+shardSize))
	dataAndCrcSize := length - r.ParityShards*(crcSize+shardSize)*blocks
	originalSize = dataAndCrcSize - CeilInt(dataAndCrcSize, crcSize+shardSize)*crcSize
	return
}

func (r *RsCrcEcc) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, crcSize, shardSize, originalSize := r.ComputeSizesFromOriginal(input.Length())
	dataSizeInBlock := r.DataShards * shardSize
	paritySizeInBlock := r.ParityShards * shardSize

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	inputBytes := inputBuffer.MakeContiguous(dataSizeInBlock * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	var eccBuffer gather.WriteBuffer
	defer eccBuffer.Close()

	eccBytes := eccBuffer.MakeContiguous(crcSize + paritySizeInBlock)

	shards := make([][]byte, r.DataShards+r.ParityShards)

	inputPos := 0

	for b := 0; b < blocks; b++ {
		eccPos := crcSize

		for i := 0; i < r.DataShards; i++ {
			shards[i] = inputBytes[inputPos : inputPos+shardSize]
			inputPos += shardSize
		}
		for i := 0; i < r.ParityShards; i++ {
			shards[r.DataShards+i] = eccBytes[eccPos : eccPos+shardSize]
			eccPos += shardSize
		}

		err := r.Enc.Encode(shards)
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}

		for i := 0; i < r.ParityShards; i++ {
			s := r.DataShards + i

			binary.BigEndian.PutUint32(eccBytes[0:crcSize], crc32.ChecksumIEEE(shards[s]))
			output.Append(eccBytes[0:crcSize])
			output.Append(shards[s])
		}
	}

	inputPos = 0

	for inputPos < originalSize {
		shard := inputBytes[inputPos : inputPos+shardSize]
		left := min(originalSize-inputPos, shardSize)
		inputPos += shardSize

		binary.BigEndian.PutUint32(eccBytes[0:crcSize], crc32.ChecksumIEEE(shard))
		output.Append(eccBytes[0:crcSize])
		output.Append(shard[:left])
	}

	return nil
}

func (r *RsCrcEcc) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, crcSize, shardSize, originalSize := r.ComputeSizesFromStored(input.Length())
	dataSizeInBlock := r.DataShards * (crcSize + shardSize)
	paritySizeInBlock := r.ParityShards * (crcSize + shardSize)

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	inputBytes := inputBuffer.MakeContiguous((dataSizeInBlock + paritySizeInBlock) * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	eccBytes := inputBytes[:paritySizeInBlock*blocks]
	dataBytes := inputBytes[paritySizeInBlock*blocks:]

	shards := make([][]byte, r.DataShards+r.ParityShards)

	dataPos := 0
	originalPos := 0
	eccPos := 0

	writeOriginalPos := 0

	for b := 0; b < blocks; b++ {
		for i := 0; i < r.DataShards; i++ {
			crc := binary.BigEndian.Uint32(dataBytes[dataPos : dataPos+crcSize])
			dataPos += crcSize

			shards[i] = dataBytes[dataPos : dataPos+shardSize]
			dataPos += shardSize

			if originalPos < originalSize && crc != crc32.ChecksumIEEE(shards[i]) {
				shards[i] = nil
			}
			originalPos += shardSize
		}
		for i := 0; i < r.ParityShards; i++ {
			s := r.DataShards + i

			crc := binary.BigEndian.Uint32(eccBytes[eccPos : eccPos+crcSize])
			eccPos += crcSize

			shards[s] = eccBytes[eccPos : eccPos+shardSize]
			eccPos += shardSize

			if crc != crc32.ChecksumIEEE(shards[s]) {
				shards[s] = nil
			}
		}

		err := r.Enc.ReconstructData(shards)
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}

		for i := 0; i < r.DataShards && writeOriginalPos < originalSize; i++ {
			left := min(originalSize-writeOriginalPos, shardSize)
			writeOriginalPos += shardSize

			output.Append(shards[i][:left])
		}
	}

	return nil
}

func (r *RsCrcEcc) Overhead() int {
	return 0
}

func init() {
	RegisterAlgorithm(RsCrc321pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsCrcEcc(opts, 1, 1024)
	})
	RegisterAlgorithm(RsCrc322pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsCrcEcc(opts, 2, 1024)
	})
	RegisterAlgorithm(RsCrc325pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsCrcEcc(opts, 5, 512)
	})
	RegisterAlgorithm(RsCrc3210pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsCrcEcc(opts, 10, 256)
	})
}
