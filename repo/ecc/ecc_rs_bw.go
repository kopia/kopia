package ecc

import (
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/pkg/errors"
	"github.com/vivint/infectious"
)

const (
	RsBwFs1pEccName  = "RS-BW-FS-1%"
	RsBwFs2pEccName  = "RS-BW-FS-2%"
	RsBwFs5pEccName  = "RS-BW-FS-5%"
	RsBwFs10pEccName = "RS-BW-FS-10%"
	RsBwFb1pEccName  = "RS-BW-FB-1%"
	RsBwFb2pEccName  = "RS-BW-FB-2%"
	RsBwFb5pEccName  = "RS-BW-FB-5%"
	RsBwFb10pEccName = "RS-BW-FB-10%"
)

// RsBwEcc implements Reed-Solomon error codes with Berlekamp-Welch error detection
type RsBwEcc struct {
	Options
	FixedShareSize  bool
	RequiredShares  int
	RedundantShares int
	Fec             *infectious.FEC
}

func NewRsBwEcc(opts *Options, fixedShareSize bool, spaceUsedPercentage float32) (*RsBwEcc, error) {
	result := new(RsBwEcc)

	result.Options = *opts
	result.FixedShareSize = fixedShareSize
	result.RequiredShares, result.RedundantShares = ComputeShards(spaceUsedPercentage)

	var err error
	result.Fec, err = infectious.NewFEC(result.RequiredShares, result.RequiredShares+result.RedundantShares)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating infectious FEC")
	}

	return result, nil
}

func (r *RsBwEcc) ComputeSizesFromOriginal(length int) (blocks, shareSize, originalSize int) {
	if r.FixedShareSize {
		shareSize = 1024
		blocks = CeilInt(length, r.RequiredShares*shareSize)
	} else {
		blocks = 16
		shareSize = CeilInt(length, r.RequiredShares*blocks)
	}
	originalSize = length
	return
}

func (r *RsBwEcc) ComputeSizesFromStored(length int) (blocks, shareSize, originalSize int) {
	if r.FixedShareSize {
		shareSize = 1024
		blocks = CeilInt(length, (r.RequiredShares+r.RedundantShares)*shareSize)
	} else {
		blocks = 16
		shareSize = CeilInt(length, (r.RequiredShares+r.RedundantShares)*blocks)
	}
	originalSize = length - r.RedundantShares*shareSize*blocks
	return
}

func (r *RsBwEcc) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, shareSize, originalSize := r.ComputeSizesFromOriginal(input.Length())
	requiredSize := r.RequiredShares * shareSize

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	inputBytes := inputBuffer.MakeContiguous(requiredSize * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	inputPos := 0

	for b := 0; b < blocks; b++ {
		block := inputBytes[inputPos : inputPos+requiredSize]
		inputPos += requiredSize

		err := r.Fec.Encode(block, func(share infectious.Share) {
			if share.Number >= r.RequiredShares {
				output.Append(share.Data)
			}
		})
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}
	}

	output.Append(inputBytes[:originalSize])

	return nil
}

func (r *RsBwEcc) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	blocks, shareSize, originalSize := r.ComputeSizesFromStored(input.Length())
	requiredSize := r.RequiredShares * shareSize
	redundantSize := r.RedundantShares * shareSize

	var inputBuffer gather.WriteBuffer
	defer inputBuffer.Close()

	inputBytes := inputBuffer.MakeContiguous((requiredSize + redundantSize) * blocks)

	copied := input.AppendToSlice(inputBytes[:0])

	if len(copied) < len(inputBytes) {
		clear(inputBytes[len(copied):])
	}

	eccBytes := inputBytes[:redundantSize*blocks]
	dataBytes := inputBytes[redundantSize*blocks:]

	shares := make([]infectious.Share, r.RequiredShares+r.RedundantShares)
	for i := 0; i < r.RequiredShares; i++ {
		shares[i].Number = i
	}
	for i := 0; i < r.RedundantShares; i++ {
		shares[r.RequiredShares+i].Number = r.RequiredShares + i
	}

	dataPos := 0
	eccPos := 0
	for b := 0; b < blocks; b++ {
		for i := 0; i < r.RequiredShares; i++ {
			shares[i].Data = dataBytes[dataPos : dataPos+shareSize]
			dataPos += shareSize
		}
		for i := 0; i < r.RedundantShares; i++ {
			shares[r.RequiredShares+i].Data = eccBytes[eccPos : eccPos+shareSize]
			eccPos += shareSize
		}

		err := r.Fec.Correct(shares)
		if err != nil {
			return errors.Wrap(err, "Error computing ECC")
		}
	}

	output.Append(dataBytes[:originalSize])

	return nil
}

func (r *RsBwEcc) Overhead() int {
	return 0
}

func init() {
	RegisterAlgorithm(RsBwFs1pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, true, 1)
	})
	RegisterAlgorithm(RsBwFs2pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, true, 2)
	})
	RegisterAlgorithm(RsBwFs5pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, true, 5)
	})
	RegisterAlgorithm(RsBwFs10pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, true, 10)
	})

	RegisterAlgorithm(RsBwFb1pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, false, 1)
	})
	RegisterAlgorithm(RsBwFb2pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, false, 2)
	})
	RegisterAlgorithm(RsBwFb5pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, false, 5)
	})
	RegisterAlgorithm(RsBwFb10pEccName, func(opts *Options) (encryption.Encryptor, error) {
		return NewRsBwEcc(opts, false, 10)
	})
}
