package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"
)

// ErrConfigNotFound indicates that configuration is not found.
var ErrConfigNotFound = errors.New("config not found")

type Session interface {
	io.Closer

	InitObjectManager(f cas.Format) (cas.ObjectManager, error)
	OpenObjectManager() (cas.ObjectManager, error)
}

type session struct {
	storage blob.Storage
	creds   auth.Credentials
	format  cas.Format
}

func (s *session) Close() error {
	return nil
}

func (s *session) getPrivateBlock(blkID string) ([]byte, error) {
	b, err := s.storage.GetBlock(blkID)
	if err != nil {
		return nil, err
	}

	return b, err
}

func (s *session) encryptBlockWithPublicKey(blkID string, data io.ReadCloser, options blob.PutOptions) error {
	err := s.storage.PutBlock(blkID, data, options)
	if err != nil {
		return fmt.Errorf("unable to write block %v: %v", blkID, err)
	}

	return err
}

func (s *session) getConfigstring() string {
	if s.creds == nil {
		return string("config.json")
	}

	return string("users." + s.creds.Username() + ".config.json")
}

func (s *session) InitObjectManager(format cas.Format) (cas.ObjectManager, error) {
	mgr, err := cas.NewObjectManager(s.storage, format)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(format)
	if err != nil {
		return nil, err
	}

	if err := s.encryptBlockWithPublicKey(
		s.getConfigstring(),
		ioutil.NopCloser(bytes.NewBuffer(b)),
		blob.PutOptions{}); err != nil {
		return nil, err
	}

	return mgr, nil
}

func (s *session) OpenObjectManager() (cas.ObjectManager, error) {
	b, err := s.getPrivateBlock(s.getConfigstring())
	if err != nil {
		if err == blob.ErrBlockNotFound {
			return nil, ErrConfigNotFound
		}
		return nil, err
	}

	var format cas.Format
	err = json.Unmarshal(b, &format)
	if err != nil {
		return nil, err
	}

	return cas.NewObjectManager(s.storage, format)
}

func New(storage blob.Storage, creds auth.Credentials) (Session, error) {
	sess := &session{
		storage: storage,
		creds:   creds,
	}
	return sess, nil
}
