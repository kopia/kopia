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
	ReedSolomonCrc321pEccName  = "REED-SOLOMON-CRC32-1%"
	ReedSolomonCrc322pEccName  = "REED-SOLOMON-CRC32-2%"
	ReedSolomonCrc325pEccName  = "REED-SOLOMON-CRC32-5%"
	ReedSolomonCrc3210pEccName = "REED-SOLOMON-CRC32-10%"
)

// ReedSolomonCrcECC implements Reed-Solomon error codes with CRC32 error detection
type ReedSolomonCrcECC struct {
	Options
	DataShards   int
	ParityShards int
	ShardSize    int
	enc          reedsolomon.Encoder
}

func NewReedSolomonCrcECC(opts *Options, spaceUsedPercentage float32, shardSize int) (*ReedSolomonCrcECC, error) {
	result := new(ReedSolomonCrcECC)

	result.Options = *opts
	result.DataShards, result.ParityShards = ComputeShards(spaceUsedPercentage)
	result.ShardSize = shardSize

	var err error
	result.enc, err = reedsolomon.New(result.DataShards, result.ParityShards,
		reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, errors.Wrap(err, "Error creating reedsolomon encoder")
	}

	return result, nil
}

func (r *ReedSolomonCrcECC) ComputeSizesFromOriginal(length int) (blocks, crcSize, shardSize, originalSize int) {
	crcSize = 4
	shardSize = r.ShardSize
	blocks = CeilInt(length, r.DataShards*shardSize)
	originalSize = length
	return
}

func (r *ReedSolomonCrcECC) ComputeSizesFromStored(length int) (blocks, crcSize, shardSize, originalSize int) {
	crcSize = 4
	shardSize = r.ShardSize
	blocks = CeilInt(length, (r.DataShards+r.ParityShards)*(crcSize+shardSize))
	dataAndCrcSize := length - r.ParityShards*(crcSize+shardSize)*blocks
	originalSize = dataAndCrcSize - CeilInt(dataAndCrcSize, crcSize+shardSize)*crcSize
	return
}

// Encrypt creates ECC for the bytes in input.
// The bytes in output are stored in with the layout:
//     ([CRC32][Parity shard])+ ([CRC32][Data shard])+
// All shards must be of the same size, so it may be needed to pad the input data.
// The parity data comes first so we can avoid storing the padding needed for the
// data shards, and instead compute the padded size based on the input length.
// All parity shards are always stored.
func (r *ReedSolomonCrcECC) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, crcSize, shardSize, originalSize := r.ComputeSizesFromOriginal(input.Length())
	dataSizeInBlock := r.DataShards * shardSize
	paritySizeInBlock := r.ParityShards * shardSize

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	// Allocate space for the input + padding
	inputBytes := inputBuffer.MakeContiguous(dataSizeInBlock * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	// WriteBuffer does not clear the data, so we must clear the padding
	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	// Compute and store ECC + checksum

	var eccBuffer gather.WriteBuffer
	defer eccBuffer.Close()

	eccBytes := eccBuffer.MakeContiguous(crcSize + paritySizeInBlock)

	var maxShards [256][]byte
	shards := maxShards[:r.DataShards+r.ParityShards]

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

		err := r.enc.Encode(shards)
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

	// Now store the original data + checksum

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

// Decrypt corrects the data from input based on the ECC data.
// See Encrypt comments for a description of the layout.
func (r *ReedSolomonCrcECC) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, crcSize, shardSize, originalSize := r.ComputeSizesFromStored(input.Length())
	dataPlusCrcSizeInBlock := r.DataShards * (crcSize + shardSize)
	parityPlusCrcSizeInBlock := r.ParityShards * (crcSize + shardSize)

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	// Allocate space for the input + padding
	inputBytes := inputBuffer.MakeContiguous((dataPlusCrcSizeInBlock + parityPlusCrcSizeInBlock) * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	// WriteBuffer does not clear the data, so we must clear the padding
	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	eccBytes := inputBytes[:parityPlusCrcSizeInBlock*blocks]
	dataBytes := inputBytes[parityPlusCrcSizeInBlock*blocks:]

	var maxShards [256][]byte
	shards := maxShards[:r.DataShards+r.ParityShards]

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
				// The data was corrupted, so we need to reconstruct it
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
				// The data was corrupted, so we need to reconstruct it
				shards[s] = nil
			}
		}

		err := r.enc.ReconstructData(shards)
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

func (r *ReedSolomonCrcECC) Overhead() int {
	return 0
}

func init() {
	RegisterAlgorithm(ReedSolomonCrc321pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewReedSolomonCrcECC(opts, 1, 1024)
	})
	RegisterAlgorithm(ReedSolomonCrc322pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewReedSolomonCrcECC(opts, 2, 1024)
	})
	RegisterAlgorithm(ReedSolomonCrc325pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewReedSolomonCrcECC(opts, 5, 512)
	})
	RegisterAlgorithm(ReedSolomonCrc3210pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewReedSolomonCrcECC(opts, 10, 256)
	})
}
