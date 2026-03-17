package database

import (
	"testing"
)

func BenchmarkDatabase_SetStringBytes_Overwrite(b *testing.B) {
	db := NewDatabase(0)
	_ = db.SetStringBytes([]byte("k"), []byte("value"))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.SetStringBytes([]byte("k"), []byte("value"))
	}
}

func BenchmarkDatabase_IncrStringBytes_Hit(b *testing.B) {
	db := NewDatabase(0)
	_, _ = db.IncrStringBytes([]byte("counter"))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.IncrStringBytes([]byte("counter"))
	}
}

