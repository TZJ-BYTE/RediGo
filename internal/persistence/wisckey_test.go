package persistence

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWiscKeyIntegration(t *testing.T) {
	dbDir := "./test_wisckey_db"
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	opts := &Options{
		MaxOpenFiles: 10,
	}
	engine, err := OpenLSMEnergy(dbDir, opts)
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// 1. 写入大量数据（触发 Value Log 写入）
	// 使用较大的 Value 以确保 vLog 效果明显
	key := []byte("big_key")
	value := make([]byte, 1024) // 1KB
	for i := range value {
		value[i] = byte(i % 256)
	}

	if err := engine.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, found := engine.Get(key)
	if !found {
		t.Fatal("Key not found")
	}
	if !bytes.Equal(val, value) {
		t.Fatal("Value mismatch")
	}

	if err := engine.flushMemTableSync(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	val, found = engine.Get(key)
	if !found {
		t.Fatal("Key not found after flush")
	}
	if !bytes.Equal(val, value) {
		t.Fatal("Value mismatch after flush")
	}

	vlogDir := filepath.Join(dbDir, "vlog")
	entries, _ := os.ReadDir(vlogDir)
	if len(entries) == 0 {
		t.Fatal("No vLog files found")
	}

	engine.Close()

	engine2, err := OpenLSMEnergy(dbDir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	val, found = engine2.Get(key)
	if !found {
		t.Fatal("Key not found after reopen")
	}
	if !bytes.Equal(val, value) {
		t.Fatal("Value mismatch after reopen")
	}
}
