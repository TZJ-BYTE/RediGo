package protocol

import "strconv"

func AppendSimpleString(dst []byte, s string) []byte {
	dst = append(dst, SimpleString)
	dst = append(dst, s...)
	dst = append(dst, '\r', '\n')
	return dst
}

func AppendErrorString(dst []byte, s string) []byte {
	dst = append(dst, Error)
	dst = append(dst, s...)
	dst = append(dst, '\r', '\n')
	return dst
}

func AppendInteger(dst []byte, v int64) []byte {
	dst = append(dst, Integer)
	dst = strconv.AppendInt(dst, v, 10)
	dst = append(dst, '\r', '\n')
	return dst
}

func AppendNull(dst []byte) []byte {
	dst = append(dst, BulkString)
	dst = append(dst, '-', '1')
	dst = append(dst, '\r', '\n')
	return dst
}

func AppendBulkString(dst []byte, s string) []byte {
	dst = append(dst, BulkString)
	dst = strconv.AppendInt(dst, int64(len(s)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, s...)
	dst = append(dst, '\r', '\n')
	return dst
}

func AppendBulkBytes(dst []byte, b []byte) []byte {
	dst = append(dst, BulkString)
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	dst = append(dst, '\r', '\n')
	return dst
}
