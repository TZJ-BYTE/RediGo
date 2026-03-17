package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestOffloading_FSBackend_ReadBack(t *testing.T) {
	tmpDir := t.TempDir()

	options := DefaultOptions()
	options.EnableOffloading = true
	options.OffloadBackend = "fs"
	options.OffloadFSRoot = filepath.Join(tmpDir, "objects")
	options.OffloadMinLevel = 0
	options.OffloadKeepLocal = true

	engine, err := OpenLSMEnergy(tmpDir, options)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if engine.compactor != nil {
		engine.compactor.Stop()
	}

	i := 0
	for round := 0; round < 5; round++ {
		for j := 0; j < 50; j++ {
			key := fmt.Sprintf("k_%d", i)
			val := make([]byte, 256)
			for k := range val {
				val[k] = byte((i + k) % 251)
			}
			if err := engine.Put([]byte(key), val); err != nil {
				t.Fatalf("put: %v", err)
			}
			i++
		}
		if err := engine.flushMemTableSync(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	version := engine.versionSet.GetCurrentVersion()
	if version == nil || len(version.Files) < 1 || len(version.Files[0]) == 0 {
		t.Fatalf("invalid version")
	}

	var fm *FileMetadata
	for _, f := range version.Files[0] {
		if f.FileNum == 1 {
			fm = f
			break
		}
	}
	if fm == nil {
		fm = version.Files[0][0]
	}
	objPath := filepath.Join(options.OffloadFSRoot, "sstable", fmt.Sprintf("%06d.sstable", fm.FileNum))
	if _, err := os.Stat(objPath); err != nil {
		t.Fatalf("expected object uploaded: %v", err)
	}

	engine.tableCache.Evict(fm.FileNum)
	localPath := filepath.Join(tmpDir, "sstable", fmt.Sprintf("%06d.sstable", fm.FileNum))
	_ = os.Remove(localPath)

	key := []byte("k_1")
	_, found := engine.Get(key)
	if !found {
		t.Fatalf("expected key found after offloading")
	}
}
