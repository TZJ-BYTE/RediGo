package datastruct

import (
	"sync"
	"time"
)

var (
	// DataValuePool DataValue 对象池
	DataValuePool = sync.Pool{
		New: func() interface{} {
			return &DataValue{}
		},
	}

	// StringPool String 对象池
	StringPool = sync.Pool{
		New: func() interface{} {
			return &String{}
		},
	}
)

// NewDataValue 从池中获取 DataValue
func NewDataValue() *DataValue {
	dv := DataValuePool.Get().(*DataValue)
	dv.LastAccessedAt = time.Now().UnixMilli()
	return dv
}

// FreeDataValue 将 DataValue 放回池中
func FreeDataValue(dv *DataValue) {
	dv.Value = nil
	dv.ExpireTime = 0
	dv.LastAccessedAt = 0
	DataValuePool.Put(dv)
}

// NewString 从池中获取 String
func NewString() *String {
	return StringPool.Get().(*String)
}

// FreeString 将 String 放回池中
func FreeString(s *String) {
	s.Data = ""
	StringPool.Put(s)
}
