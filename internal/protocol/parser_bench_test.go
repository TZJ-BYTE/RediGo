package protocol

import "testing"

func BenchmarkEncodeResponse_SimpleString(b *testing.B) {
	resp := MakeSimpleString("OK")
	b.ReportAllocs()
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf = EncodeResponseInto(buf, resp)
	}
}

func BenchmarkEncodeResponse_Integer(b *testing.B) {
	resp := MakeInteger(123456789)
	b.ReportAllocs()
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf = EncodeResponseInto(buf, resp)
	}
}

func BenchmarkEncodeResponse_Array(b *testing.B) {
	resp := MakeArray([]string{"a", "b", "c", "d", "e", "f", "g"})
	b.ReportAllocs()
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf = EncodeResponseInto(buf, resp)
	}
}

func BenchmarkParseOneRequestFast(b *testing.B) {
	payload := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, n, err := ParseOneRequestFast(payload)
		if err != nil || n == 0 || req == nil {
			b.Fatal(err)
		}
		ReleaseRequest(req)
	}
}

func BenchmarkAppendResponse_SimpleString(b *testing.B) {
	resp := MakeSimpleString("OK")
	b.ReportAllocs()
	buf := make([]byte, 0, 4096)
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = AppendResponse(buf, resp)
	}
}
