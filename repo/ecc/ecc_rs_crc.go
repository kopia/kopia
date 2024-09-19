package ecc

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
)

const (
	// AlgorithmReedSolomonWithCrc32 is the name of an implemented algorithm.
	AlgorithmReedSolomonWithCrc32 = "REED-SOLOMON-CRC32"

	lengthSize             = 4
	crcSize                = 4
	smallFilesDataShards   = 6
	smallFilesParityShards = 2
)

// ReedSolomonCrcECC implements Reed-Solomon error codes with CRC32 error detection.
type ReedSolomonCrcECC struct {
	Options
	DataShards            int
	ParityShards          int
	ThresholdParityInput  int
	ThresholdParityOutput int
	ThresholdBlocksInput  int
	ThresholdBlocksOutput int
	encSmallFiles         reedsolomon.Encoder
	encBigFiles           reedsolomon.Encoder
}

func newReedSolomonCrcECC(opts *Options) (*ReedSolomonCrcECC, error) {
	result := new(ReedSolomonCrcECC)

	result.Options = *opts

	if opts.MaxShardSize == 0 {
		switch {
		case opts.OverheadPercent == 1:
			result.MaxShardSize = 1024

		case opts.OverheadPercent == 2: //nolint:mnd
			result.MaxShardSize = 512

		case opts.OverheadPercent == 3: //nolint:mnd
			result.MaxShardSize = 256

		case opts.OverheadPercent <= 6: //nolint:mnd
			result.MaxShardSize = 128

		default:
			result.MaxShardSize = 64
		}
	}

	// Remove the space used for the crc from the allowed space overhead, if possible
	freeSpaceOverhead := float32(opts.OverheadPercent) - 100*crcSize/float32(result.MaxShardSize)
	freeSpaceOverhead = maxFloat32(freeSpaceOverhead, 0.01) //nolint:mnd
	result.DataShards, result.ParityShards = computeShards(freeSpaceOverhead)

	// Bellow this threshold the data will be split in less shards
	result.ThresholdParityInput = 2 * crcSize * (result.DataShards + result.ParityShards) //nolint:mnd
	result.ThresholdParityOutput = computeFinalFileSizeWithPadding(smallFilesDataShards, smallFilesParityShards, ceilInt(result.ThresholdParityInput, smallFilesDataShards), 1)

	// Bellow this threshold the shard size will shrink to the smallest possible
	result.ThresholdBlocksInput = result.DataShards * result.MaxShardSize
	result.ThresholdBlocksOutput = computeFinalFileSizeWithPadding(result.DataShards, result.ParityShards, result.MaxShardSize, 1)

	var err error

	result.encBigFiles, err = reedsolomon.New(result.DataShards, result.ParityShards,
		reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, errors.Wrap(err, "Error creating reedsolomon encoder")
	}

	result.encSmallFiles, err = reedsolomon.New(smallFilesDataShards, smallFilesParityShards,
		reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, errors.Wrap(err, "Error creating reedsolomon encoder")
	}

	return result, nil
}

func (r *ReedSolomonCrcECC) computeSizesFromOriginal(length int) sizesInfo {
	length += lengthSize

	var result sizesInfo

	switch {
	case length <= r.ThresholdParityInput:
		result.Blocks = 1
		result.DataShards = smallFilesDataShards
		result.ParityShards = smallFilesParityShards
		result.ShardSize = ceilInt(length, result.DataShards)
		result.StorePadding = true
		result.enc = r.encSmallFiles

	case length <= r.ThresholdBlocksInput:
		result.Blocks = 1
		result.DataShards = r.DataShards
		result.ParityShards = r.ParityShards
		result.ShardSize = ceilInt(length, result.DataShards)
		result.StorePadding = true
		result.enc = r.encBigFiles

	default:
		result.ShardSize = r.MaxShardSize
		result.DataShards = r.DataShards
		result.ParityShards = r.ParityShards
		result.Blocks = ceilInt(length, result.DataShards*result.ShardSize)
		result.StorePadding = false
		result.enc = r.encBigFiles
	}

	return result
}

func (r *ReedSolomonCrcECC) computeSizesFromStored(length int) sizesInfo {
	var result sizesInfo

	switch {
	case length <= r.ThresholdParityOutput:
		result.Blocks = 1
		result.DataShards = smallFilesDataShards
		result.ParityShards = smallFilesParityShards
		result.ShardSize = maxInt(ceilInt(length, result.DataShards+result.ParityShards), 1+crcSize) - crcSize
		result.StorePadding = true
		result.enc = r.encSmallFiles

	case length <= r.ThresholdBlocksOutput:
		result.Blocks = 1
		result.DataShards = r.DataShards
		result.ParityShards = r.ParityShards
		result.ShardSize = maxInt(ceilInt(length, result.DataShards+result.ParityShards), 1+crcSize) - crcSize
		result.StorePadding = true
		result.enc = r.encBigFiles

	default:
		result.DataShards = r.DataShards
		result.ParityShards = r.ParityShards
		result.ShardSize = r.MaxShardSize
		result.Blocks = ceilInt(length, (result.DataShards+result.ParityShards)*(crcSize+result.ShardSize))
		result.StorePadding = false
		result.enc = r.encBigFiles
	}

	return result
}

// Encrypt creates ECC for the bytes in input.
// The bytes in output are stored in with the layout:
// ([CRC32][Parity shard])+ ([CRC32][Data shard])+
// With one detail: the length of the original data is prepended to the data itself,
// so that we can know it when storing also the padding.
// All shards must be of the same size, so it may be needed to pad the input data.
// The parity data comes first so we can avoid storing the padding needed for the
// data shards, and instead compute the padded size based on the input length.
// All parity shards are always stored.
func (r *ReedSolomonCrcECC) Encrypt(input gather.Bytes, _ []byte, output *gather.WriteBuffer) error {
	sizes := r.computeSizesFromOriginal(input.Length())
	inputPlusLengthSize := lengthSize + input.Length()
	dataSizeInBlock := sizes.DataShards * sizes.ShardSize
	paritySizeInBlock := sizes.ParityShards * sizes.ShardSize

	// Allocate space for the input + padding
	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()
	inputBytes := inputBuffer.MakeContiguous(dataSizeInBlock * sizes.Blocks)

	binary.BigEndian.PutUint32(inputBytes[:lengthSize], uint32(input.Length())) //nolint:gosec
	copied := input.AppendToSlice(inputBytes[lengthSize:lengthSize])

	// WriteBuffer does not clear the data, so we must clear the padding
	if lengthSize+len(copied) < len(inputBytes) {
		fillWithZeros(inputBytes[lengthSize+len(copied):])
	}

	// Compute and store ECC + checksum

	var crcBuffer [crcSize]byte
	crcBytes := crcBuffer[:]

	var eccBuffer gather.WriteBuffer
	defer eccBuffer.Close()
	eccBytes := eccBuffer.MakeContiguous(paritySizeInBlock)

	var maxShards [256][]byte
	shards := maxShards[:sizes.DataShards+sizes.ParityShards]

	inputPos := 0

	for range sizes.Blocks {
		eccPos := 0

		for i := range sizes.DataShards {
			shards[i] = inputBytes[inputPos : inputPos+sizes.ShardSize]
			inputPos += sizes.ShardSize
		}

		for i := range sizes.ParityShards {
			shards[sizes.DataShards+i] = eccBytes[eccPos : eccPos+sizes.ShardSize]
			eccPos += sizes.ShardSize
		}

		err := sizes.enc.Encode(shards)
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}

		for i := range sizes.ParityShards {
			s := sizes.DataShards + i

			binary.BigEndian.PutUint32(crcBytes, crc32.ChecksumIEEE(shards[s]))
			output.Append(crcBytes)
			output.Append(shards[s])
		}
	}

	// Now store the original data + checksum

	inputPos = 0

	inputSizeToStore := len(inputBytes)
	if !sizes.StorePadding {
		inputSizeToStore = inputPlusLengthSize
	}

	for inputPos < inputSizeToStore {
		left := minInt(inputSizeToStore-inputPos, sizes.ShardSize)
		shard := inputBytes[inputPos : inputPos+sizes.ShardSize]
		inputPos += sizes.ShardSize

		binary.BigEndian.PutUint32(crcBytes, crc32.ChecksumIEEE(shard))
		output.Append(crcBytes)
		output.Append(shard[:left])
	}

	return nil
}

// Decrypt corrects the data from input based on the ECC data.
// See Encrypt comments for a description of the layout.
func (r *ReedSolomonCrcECC) Decrypt(input gather.Bytes, _ []byte, output *gather.WriteBuffer) error {
	sizes := r.computeSizesFromStored(input.Length())
	dataPlusCrcSizeInBlock := sizes.DataShards * (crcSize + sizes.ShardSize)
	parityPlusCrcSizeInBlock := sizes.ParityShards * (crcSize + sizes.ShardSize)

	// Allocate space for the input + padding
	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()
	inputBytes := inputBuffer.MakeContiguous((dataPlusCrcSizeInBlock + parityPlusCrcSizeInBlock) * sizes.Blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	// WriteBuffer does not clear the data, so we must clear the padding
	if len(copied) < len(inputBytes) {
		fillWithZeros(inputBytes[len(copied):])
	}

	eccBytes := inputBytes[:parityPlusCrcSizeInBlock*sizes.Blocks]
	dataBytes := inputBytes[parityPlusCrcSizeInBlock*sizes.Blocks:]

	var maxShards [256][]byte
	shards := maxShards[:sizes.DataShards+sizes.ParityShards]

	dataPos := 0
	eccPos := 0

	var originalSize int

	writeOriginalPos := 0
	paddingStartPos := len(copied) - parityPlusCrcSizeInBlock*sizes.Blocks

	for b := range sizes.Blocks {
		for i := range sizes.DataShards {
			initialDataPos := dataPos

			crc := binary.BigEndian.Uint32(dataBytes[dataPos : dataPos+crcSize])
			dataPos += crcSize

			shards[i] = dataBytes[dataPos : dataPos+sizes.ShardSize]
			dataPos += sizes.ShardSize

			// We don't need to compute the crc inside the padding
			if initialDataPos < paddingStartPos {
				if crc != crc32.ChecksumIEEE(shards[i]) {
					// The data was corrupted, so we need to reconstruct it
					shards[i] = nil
				}
			}
		}

		for i := range sizes.ParityShards {
			s := sizes.DataShards + i

			crc := binary.BigEndian.Uint32(eccBytes[eccPos : eccPos+crcSize])
			eccPos += crcSize

			shards[s] = eccBytes[eccPos : eccPos+sizes.ShardSize]
			eccPos += sizes.ShardSize

			if crc != crc32.ChecksumIEEE(shards[s]) {
				// The data was corrupted, so we need to reconstruct it
				shards[s] = nil
			}
		}

		if r.Options.DeleteFirstShardForTests {
			shards[0] = nil
		}

		err := sizes.enc.ReconstructData(shards)
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}

		startShard := 0
		startByte := 0

		if b == 0 {
			originalSize, startShard, startByte = readLength(shards, &sizes)
		}

		// Copy data to output. Because we prepend the original file length to the
		// data, we need to ignore it
		for i := startShard; i < sizes.DataShards && writeOriginalPos < originalSize; i++ {
			left := minInt(originalSize-writeOriginalPos, sizes.ShardSize-startByte)
			writeOriginalPos += left

			output.Append(shards[i][startByte : startByte+left])

			startByte = 0
		}
	}

	return nil
}

func readLength(shards [][]byte, sizes *sizesInfo) (originalSize, startShard, startByte int) {
	var lengthBuffer [lengthSize]byte

	switch sizes.ShardSize {
	case 1:
		startShard = 4
		startByte = 0

		for i := range 4 {
			lengthBuffer[i] = shards[i][0]
		}

	case 2: //nolint:mnd
		startShard = 2
		startByte = 0

		copy(lengthBuffer[0:2], shards[0])
		copy(lengthBuffer[2:4], shards[1])

	case 3: //nolint:mnd
		startShard = 1
		startByte = 1

		copy(lengthBuffer[0:3], shards[0])
		copy(lengthBuffer[3:4], shards[1])

	case 4: //nolint:mnd
		startShard = 1
		startByte = 0

		copy(lengthBuffer[0:4], shards[0])

	default:
		startShard = 0
		startByte = 4

		copy(lengthBuffer[0:4], shards[0][:4])
	}

	originalSize = int(binary.BigEndian.Uint32(lengthBuffer[:]))

	//nolint:nakedret
	return
}

// Overhead should not be called. It's just implemented because it is in the interface.
func (r *ReedSolomonCrcECC) Overhead() int {
	panic("Should not be called")
}

type sizesInfo struct {
	Blocks       int
	ShardSize    int
	DataShards   int
	ParityShards int
	StorePadding bool
	enc          reedsolomon.Encoder
}

func computeFinalFileSizeWithPadding(dataShards, parityShards, shardSize, blocks int) int {
	return (parityShards + dataShards) * (crcSize + shardSize) * blocks
}

func init() {
	RegisterAlgorithm(AlgorithmReedSolomonWithCrc32, func(opts *Options) (encryption.Encryptor, error) {
		return newReedSolomonCrcECC(opts)
	})
}
