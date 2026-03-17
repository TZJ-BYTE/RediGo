package protocol

import "unsafe"

func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func BytesToStringCopy(b []byte) string {
	return string(b)
}

func ParseInt(b []byte) (int, error) {
	return parseIntBytes(b)
}

