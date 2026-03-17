package database

import (
	"testing"

	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
)

func BenchmarkDatabase_GetBytes_Hit(b *testing.B) {
	db := NewDatabase(0)
	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "value"}})
	k := []byte("k")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.GetBytes(k)
	}
}

func BenchmarkDatabase_SetBytes_Overwrite(b *testing.B) {
	db := NewDatabase(0)
	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "value"}})
	k := []byte("k")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.SetBytes(k, &datastruct.DataValue{Value: &datastruct.String{Data: "value"}})
	}
}

