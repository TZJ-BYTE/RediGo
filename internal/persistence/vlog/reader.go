package vlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

// ValueLogReader Value Log 读取器
type ValueLogReader struct {
	dirPath string
	files   map[uint32]*os.File
	mu      sync.RWMutex
}

// NewValueLogReader 创建新的 Value Log 读取器
func NewValueLogReader(dirPath string) *ValueLogReader {
	return &ValueLogReader{
		dirPath: dirPath,
		files:   make(map[uint32]*os.File),
	}
}

// Read 根据 ValuePointer 读取 Value
func (r *ValueLogReader) Read(vp *ValuePointer) ([]byte, error) {
	if vp == nil {
		return nil, fmt.Errorf("invalid value pointer")
	}

	r.mu.RLock()
	file, exists := r.files[vp.Fid]
	r.mu.RUnlock()

	if !exists {
		// ... (打开文件逻辑不变)
		r.mu.Lock()
		if f, ok := r.files[vp.Fid]; ok {
			file = f
		} else {
			filename := fmt.Sprintf("%s/%06d.vlog", r.dirPath, vp.Fid)
			f, err := os.Open(filename)
			if err != nil {
				r.mu.Unlock()
				return nil, err
			}
			r.files[vp.Fid] = f
			file = f
		}
		r.mu.Unlock()
	}

	// 1. 读取头部 [KeyLen(4)][ValueLen(4)]
	header := make([]byte, 8)
	_, err := file.ReadAt(header, vp.Offset)
	if err != nil {
		return nil, err
	}

	keyLen := binary.BigEndian.Uint32(header[0:4])
	valLen := binary.BigEndian.Uint32(header[4:8])

	// 校验长度是否匹配
	// 格式: [KeyLen(4)][ValueLen(4)][Key][Value][Checksum(4)]
	expectedLen := 8 + keyLen + valLen + 4
	if uint32(vp.Len) != expectedLen {
		// 可能是旧格式或者数据损坏
		// 如果我们是在升级过程中，可能需要兼容旧格式？
		// 为了简单，我们只支持带 Checksum 的新格式。
		// 如果长度不匹配，尝试按旧格式读取（无 Checksum）
		oldExpectedLen := 8 + keyLen + valLen
		if uint32(vp.Len) == oldExpectedLen {
			// 旧格式，直接读取 Value
			value := make([]byte, valLen)
			_, err = file.ReadAt(value, vp.Offset+8+int64(keyLen))
			if err != nil {
				return nil, err
			}
			return value, nil
		}

		return nil, fmt.Errorf("value pointer length mismatch: expected %d, got %d", expectedLen, vp.Len)
	}

	// 2. 读取完整 Entry 进行校验
	// 注意：为了性能，我们可以只读 Value，但这会失去 Checksum 的意义。
	// 这里我们读取整个 Entry 来校验。
	entryData := make([]byte, expectedLen)
	_, err = file.ReadAt(entryData, vp.Offset)
	if err != nil {
		return nil, err
	}

	storedChecksum := binary.BigEndian.Uint32(entryData[expectedLen-4:])
	calculatedChecksum := crc32.ChecksumIEEE(entryData[:expectedLen-4])

	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %x, got %x", storedChecksum, calculatedChecksum)
	}

	// 3. 提取 Value
	// Value 位于 Offset + 8 + KeyLen 处
	// 在 entryData 中的偏移也是 8 + KeyLen
	value := make([]byte, valLen)
	copy(value, entryData[8+keyLen:8+keyLen+valLen])

	return value, nil
}

// Close 关闭读取器
func (r *ValueLogReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, file := range r.files {
		file.Close()
	}
	r.files = make(map[uint32]*os.File)
	return nil
}
