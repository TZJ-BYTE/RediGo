package persistence

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestWiscKeyGCIntegration(t *testing.T) {
	dbDir := "./test_wisckey_gc_db"
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	opts := DefaultOptions()
	engine, err := OpenLSMEnergy(dbDir, opts)
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	key1 := []byte("key1")
	val1 := []byte("value1_should_be_kept")
	if err := engine.Put(key1, val1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("garbage_key_%d", i))
		v := make([]byte, 1024) // 1KB
		_ = engine.Put(k, v)
		_ = engine.Delete(k)
	}

	val, found := engine.Get(key1)
	if !found {
		t.Fatal("Key1 lost")
	}
	if !bytes.Equal(val, val1) {
		t.Fatal("Value1 mismatch")
	}
}
