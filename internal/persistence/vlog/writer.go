package vlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

// ValuePointer 指向 Value Log 中的位置
type ValuePointer struct {
	Fid    uint32 // Value Log 文件 ID
	Len    uint32 // Value 长度
	Offset int64  // 文件内偏移量
}

// Encode 编码 ValuePointer 为字节数组
// 格式: [Fid(4)][Len(4)][Offset(8)] = 16 bytes
func (vp *ValuePointer) Encode() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], vp.Fid)
	binary.BigEndian.PutUint32(buf[4:8], vp.Len)
	binary.BigEndian.PutUint64(buf[8:16], uint64(vp.Offset))
	return buf
}

// DecodeValuePointer 解码字节数组为 ValuePointer
func DecodeValuePointer(data []byte) *ValuePointer {
	if len(data) != 16 {
		return nil
	}
	return &ValuePointer{
		Fid:    binary.BigEndian.Uint32(data[0:4]),
		Len:    binary.BigEndian.Uint32(data[4:8]),
		Offset: int64(binary.BigEndian.Uint64(data[8:16])),
	}
}

// ValueLogWriter Value Log 写入器
type ValueLogWriter struct {
	dirPath     string
	activeFile  *os.File
	activeFid   uint32
	maxFileSize int64
	offset      int64
	mu          sync.Mutex
}

// NewValueLogWriter 创建新的 Value Log 写入器
func NewValueLogWriter(dirPath string, maxFileSize int64) (*ValueLogWriter, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	writer := &ValueLogWriter{
		dirPath:     dirPath,
		maxFileSize: maxFileSize,
		activeFid:   0,
	}

	// 打开或创建第一个文件
	if err := writer.openActiveFile(); err != nil {
		return nil, err
	}

	return writer, nil
}

// openActiveFile 打开当前活跃文件，如果已满则创建新文件
func (w *ValueLogWriter) openActiveFile() error {
	filename := fmt.Sprintf("%s/%06d.vlog", w.dirPath, w.activeFid)
	
	// 检查当前文件是否存在且是否已满
	info, err := os.Stat(filename)
	if err == nil {
		if info.Size() >= w.maxFileSize {
			w.activeFid++
			return w.openActiveFile()
		}
		// 追加模式打开
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w.activeFile = file
		w.offset = info.Size()
	} else if os.IsNotExist(err) {
		// 创建新文件
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w.activeFile = file
		w.offset = 0
	} else {
		return err
	}
	
	return nil
}

// Write 写入 Key 和 Value 到 Log，返回 ValuePointer
// 格式: [KeyLen(4)][ValueLen(4)][Key][Value][Checksum(4)]
func (w *ValueLogWriter) Write(key, value []byte) (*ValuePointer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entrySize := 4 + 4 + len(key) + len(value) + 4 // +4 for Checksum

	// 检查是否需要轮转文件
	if w.offset+int64(entrySize) > w.maxFileSize {
		if err := w.rotateFile(); err != nil {
			return nil, err
		}
	}

	// 构造 Buffer
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(value)))
	copy(buf[8:], key)
	copy(buf[8+len(key):], value)
	
	// 计算并写入 Checksum
	// Checksum 覆盖 [KeyLen, ValueLen, Key, Value]
	checksum := crc32.ChecksumIEEE(buf[:entrySize-4])
	binary.BigEndian.PutUint32(buf[entrySize-4:], checksum)

	// 写入数据
	n, err := w.activeFile.Write(buf)
	if err != nil {
		return nil, err
	}

	vp := &ValuePointer{
		Fid:    w.activeFid,
		Len:    uint32(n),
		Offset: w.offset,
	}

	w.offset += int64(n)
	return vp, nil
}

// rotateFile 轮转到下一个文件
func (w *ValueLogWriter) rotateFile() error {
	if w.activeFile != nil {
		// 同步并关闭当前文件
		if err := w.activeFile.Sync(); err != nil {
			return err
		}
		if err := w.activeFile.Close(); err != nil {
			return err
		}
	}

	w.activeFid++
	return w.openActiveFile()
}

// Close 关闭写入器
func (w *ValueLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.activeFile != nil {
		w.activeFile.Sync()
		return w.activeFile.Close()
	}
	return nil
}
