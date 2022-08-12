package encryption

import "github.com/kopia/kopia/internal/gather"

// Pipeline creates a pipeline encryptor, where the output of one becomes the input of the next.
func Pipeline(es ...Encryptor) Encryptor {
	var result Encryptor

	for i := len(es) - 1; i >= 0; i-- {
		if es[i] == nil {
			continue
		}

		if result == nil {
			result = es[i]
		} else {
			result = &pipelineStep{
				impl: es[i],
				next: result,
			}
		}
	}

	return result
}

type pipelineStep struct {
	impl Encryptor
	next Encryptor
}

func (p *pipelineStep) Encrypt(plainText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := p.impl.Encrypt(plainText, contentID, &tmp); err != nil {
		//nolint:wrapcheck
		return err
	}

	//nolint:wrapcheck
	return p.next.Encrypt(tmp.Bytes(), contentID, output)
}

func (p *pipelineStep) Decrypt(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := p.next.Decrypt(cipherText, contentID, &tmp); err != nil {
		//nolint:wrapcheck
		return err
	}

	//nolint:wrapcheck
	return p.impl.Decrypt(tmp.Bytes(), contentID, output)
}

func (p *pipelineStep) Overhead() int {
	return p.impl.Overhead() + p.next.Overhead()
}
