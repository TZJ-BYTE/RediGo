package database

import (
	"sync/atomic"
	"testing"

	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
)

func TestGetSetBytes(t *testing.T) {
	db := NewDatabase(0)
	err := db.SetBytes([]byte("k"), &datastruct.DataValue{Value: &datastruct.String{Data: "v"}})
	if err != nil {
		t.Fatalf("setbytes: %v", err)
	}
	v, ok := db.GetBytes([]byte("k"))
	if !ok {
		t.Fatalf("expected exists")
	}
	s, ok2 := v.Value.(*datastruct.String)
	if !ok2 || s.Data != "v" {
		t.Fatalf("unexpected value: %#v", v.Value)
	}
	if !db.DeleteBytes([]byte("k")) {
		t.Fatalf("expected deleted")
	}
	if _, ok := db.GetBytes([]byte("k")); ok {
		t.Fatalf("expected missing")
	}
}

func TestSetOverwriteMemoryAccounting(t *testing.T) {
	db := NewDatabase(0)
	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "a"}})
	m1 := atomic.LoadInt64(&db.usedMemory)
	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "b"}})
	m2 := atomic.LoadInt64(&db.usedMemory)
	if m1 != m2 {
		t.Fatalf("expected usedMemory unchanged for equal-size overwrite: %d vs %d", m1, m2)
	}

	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "bb"}})
	m3 := atomic.LoadInt64(&db.usedMemory)
	if m3 <= m2 {
		t.Fatalf("expected usedMemory increase when value grows: %d vs %d", m3, m2)
	}
}

