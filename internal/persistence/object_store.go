package persistence

import (
	"io"
	"os"
	"path/filepath"
)

type ObjectStore interface {
	PutObject(key string, r io.Reader) error
	GetObject(key string) (io.ReadCloser, error)
	StatObject(key string) (bool, error)
}

type FileObjectStore struct {
	root string
}

func NewFileObjectStore(root string) *FileObjectStore {
	return &FileObjectStore{root: root}
}

func (s *FileObjectStore) PutObject(key string, r io.Reader) error {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (s *FileObjectStore) GetObject(key string) (io.ReadCloser, error) {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	return os.Open(path)
}

func (s *FileObjectStore) StatObject(key string) (bool, error) {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
