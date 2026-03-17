package persistence

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type SSTableOffloader struct {
	enabled   bool
	minLevel  int
	keepLocal bool

	sstableDir string
	store      ObjectStore
}

func NewSSTableOffloader(options *Options, sstableDir string) (*SSTableOffloader, error) {
	if options == nil || !options.EnableOffloading {
		return &SSTableOffloader{enabled: false}, nil
	}

	minLevel := options.OffloadMinLevel
	if minLevel < 0 {
		minLevel = 0
	}

	var store ObjectStore
	switch options.OffloadBackend {
	case "fs":
		store = NewFileObjectStore(options.OffloadFSRoot)
	case "minio":
		s, err := NewMinioObjectStoreFromOptions(options)
		if err != nil {
			return nil, err
		}
		store = s
	default:
		return nil, fmt.Errorf("unknown offload backend: %s", options.OffloadBackend)
	}

	return &SSTableOffloader{
		enabled:   true,
		minLevel:  minLevel,
		keepLocal: options.OffloadKeepLocal,
		sstableDir: sstableDir,
		store:      store,
	}, nil
}

func (o *SSTableOffloader) keyFor(fileNum uint64) string {
	return fmt.Sprintf("sstable/%06d.sstable", fileNum)
}

func (o *SSTableOffloader) localPath(fileNum uint64) string {
	return filepath.Join(o.sstableDir, fmt.Sprintf("%06d.sstable", fileNum))
}

func (o *SSTableOffloader) OffloadIfNeeded(fm *FileMetadata) error {
	if !o.enabled || fm == nil {
		return nil
	}
	if fm.Level < o.minLevel {
		return nil
	}

	local := o.localPath(fm.FileNum)
	f, err := os.Open(local)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	if err := o.store.PutObject(o.keyFor(fm.FileNum), f); err != nil {
		return err
	}

	if !o.keepLocal {
		_ = os.Remove(local)
	}
	return nil
}

func (o *SSTableOffloader) EnsureLocal(fileNum uint64) error {
	if !o.enabled {
		return os.ErrNotExist
	}

	local := o.localPath(fileNum)
	if _, err := os.Stat(local); err == nil {
		return nil
	}

	ok, err := o.store.StatObject(o.keyFor(fileNum))
	if err != nil {
		return err
	}
	if !ok {
		return os.ErrNotExist
	}

	rc, err := o.store.GetObject(o.keyFor(fileNum))
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := os.MkdirAll(filepath.Dir(local), 0755); err != nil {
		return err
	}

	tmp := local + ".download"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, rc)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}

	return os.Rename(tmp, local)
}

