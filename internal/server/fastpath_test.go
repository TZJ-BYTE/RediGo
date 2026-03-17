package server

import (
	"testing"

	"github.com/TZJ-BYTE/RediGo/internal/database"
	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
)

func TestFastPath_SET_GET(t *testing.T) {
	db := database.NewDatabase(0)

	var buf []byte
	buf, ok := fastPathExecute(buf, db, "SET", [][]byte{[]byte("k"), []byte("value")})
	if !ok {
		t.Fatalf("expected handled")
	}
	if string(buf) != "+OK\r\n" {
		t.Fatalf("unexpected resp: %q", string(buf))
	}

	buf = buf[:0]
	buf, ok = fastPathExecute(buf, db, "GET", [][]byte{[]byte("k")})
	if !ok {
		t.Fatalf("expected handled")
	}
	if string(buf) != "$5\r\nvalue\r\n" {
		t.Fatalf("unexpected resp: %q", string(buf))
	}
}

func TestFastPath_GET_Null(t *testing.T) {
	db := database.NewDatabase(0)

	var buf []byte
	buf, ok := fastPathExecute(buf, db, "GET", [][]byte{[]byte("missing")})
	if !ok {
		t.Fatalf("expected handled")
	}
	if string(buf) != "$-1\r\n" {
		t.Fatalf("unexpected resp: %q", string(buf))
	}
}

func TestFastPath_INCR(t *testing.T) {
	db := database.NewDatabase(0)

	var buf []byte
	buf, ok := fastPathExecute(buf, db, "INCR", [][]byte{[]byte("counter")})
	if !ok {
		t.Fatalf("expected handled")
	}
	if string(buf) != ":1\r\n" {
		t.Fatalf("unexpected resp: %q", string(buf))
	}

	v, exists := db.Get("counter")
	if !exists {
		t.Fatalf("expected counter exists")
	}
	switch vv := v.Value.(type) {
	case *datastruct.String:
		if vv.Data != "1" {
			t.Fatalf("unexpected stored value: %#v", v.Value)
		}
	case *datastruct.BytesString:
		if string(vv.Data) != "1" {
			t.Fatalf("unexpected stored value: %#v", v.Value)
		}
	default:
		t.Fatalf("unexpected stored value: %#v", v.Value)
	}
}

func TestFastPath_INCR_NotInt(t *testing.T) {
	db := database.NewDatabase(0)
	_ = db.Set("counter", &datastruct.DataValue{Value: &datastruct.String{Data: "x"}})

	var buf []byte
	buf, ok := fastPathExecute(buf, db, "INCR", [][]byte{[]byte("counter")})
	if !ok {
		t.Fatalf("expected handled")
	}
	if string(buf) != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("unexpected resp: %q", string(buf))
	}
}
