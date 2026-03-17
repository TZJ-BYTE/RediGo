package server

import (
	"testing"

	"github.com/TZJ-BYTE/RediGo/internal/database"
	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
)

func BenchmarkFastPath_GET(b *testing.B) {
	db := database.NewDatabase(0)
	_ = db.Set("k", &datastruct.DataValue{Value: &datastruct.String{Data: "value"}})
	args := [][]byte{[]byte("k")}

	b.ReportAllocs()
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, _ = fastPathExecute(buf, db, "GET", args)
	}
}

func BenchmarkFastPath_SET(b *testing.B) {
	db := database.NewDatabase(0)
	args := [][]byte{[]byte("k"), []byte("value")}

	b.ReportAllocs()
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, _ = fastPathExecute(buf, db, "SET", args)
	}
}

func BenchmarkFastPath_INCR(b *testing.B) {
	db := database.NewDatabase(0)
	_ = db.Set("counter", &datastruct.DataValue{Value: &datastruct.String{Data: "1"}})
	args := [][]byte{[]byte("counter")}

	b.ReportAllocs()
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, _ = fastPathExecute(buf, db, "INCR", args)
	}
}

