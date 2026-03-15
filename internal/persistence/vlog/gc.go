package vlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// ValueLogGC 垃圾回收器
type ValueLogGC struct {
	dirPath      string
	gcThreshold  float64 // 垃圾回收阈值 (例如 0.5 表示 50% 空间无效时回收)
	checkKeyFunc func(key []byte, vp *ValuePointer) (bool, error) // 回调函数：检查 Key 指向的 VP 是否依然有效
	rewriteFunc  func(key, value []byte) error // 回调函数：重写有效数据
}

// NewValueLogGC 创建 GC
func NewValueLogGC(dirPath string, threshold float64, checkFunc func([]byte, *ValuePointer) (bool, error), rewriteFunc func([]byte, []byte) error) *ValueLogGC {
	return &ValueLogGC{
		dirPath:      dirPath,
		gcThreshold:  threshold,
		checkKeyFunc: checkFunc,
		rewriteFunc:  rewriteFunc,
	}
}

// RunGC 执行一次 GC
// 策略：
// 1. 遍历所有 vLog 文件（除了当前活跃的）
// 2. 对每个文件，随机采样或全量扫描，计算有效数据比例
// 3. 如果有效比例低于阈值，则重写该文件：
//    - 读取有效数据
//    - 写入当前活跃 vLog
//    - 更新 LSM Tree 中的索引
//    - 删除旧文件
func (gc *ValueLogGC) RunGC() error {
	files, err := gc.listFiles()
	if err != nil {
		return err
	}

	if len(files) <= 1 {
		return nil // 只有一个文件（或者是空的），无需 GC
	}

	// 简单策略：总是尝试清理最旧的文件（除了最新的）
	// 排除最新的活跃文件（ID最大）
	targetFile := files[0] // ID 最小的文件
	
	// 如果是活跃文件，跳过
	if targetFile == files[len(files)-1] {
		return nil
	}

	return gc.compactFile(targetFile)
}

// listFiles 获取所有 vLog 文件 ID，按从小到大排序
func (gc *ValueLogGC) listFiles() ([]uint32, error) {
	entries, err := os.ReadDir(gc.dirPath)
	if err != nil {
		return nil, err
	}

	var fids []uint32
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".vlog") {
			var fid uint32
			_, err := fmt.Sscanf(entry.Name(), "%06d.vlog", &fid)
			if err == nil {
				fids = append(fids, fid)
			}
		}
	}
	sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })
	return fids, nil
}

// compactFile 压缩单个文件
func (gc *ValueLogGC) compactFile(fid uint32) error {
	filename := fmt.Sprintf("%s/%06d.vlog", gc.dirPath, fid)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	
	validSize := int64(0)
	var validEntries []struct {
		key   []byte
		value []byte
	}

	// 遍历文件
	offset := int64(0)
	header := make([]byte, 8)
	for offset < fileSize {
		// 读取头部
		_, err := file.ReadAt(header, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		
		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])
		entrySize := 8 + int64(keyLen) + int64(valLen)
		
		// 读取 Key
		key := make([]byte, keyLen)
		_, err = file.ReadAt(key, offset+8)
		if err != nil {
			return err
		}
		
		// 检查 Key 是否有效
		// 构造当前 Entry 的 ValuePointer
		vp := &ValuePointer{
			Fid:    fid,
			Len:    uint32(entrySize),
			Offset: offset,
		}
		
		isValid, err := gc.checkKeyFunc(key, vp)
		if err != nil {
			return err
		}
		
		if isValid {
			validSize += entrySize
			// 读取 Value
			value := make([]byte, valLen)
			_, err = file.ReadAt(value, offset+8+int64(keyLen))
			if err != nil {
				return err
			}
			validEntries = append(validEntries, struct {
				key   []byte
				value []byte
			}{key, value})
		}
		
		offset += entrySize
	}
	
	// 计算有效率
	ratio := float64(validSize) / float64(fileSize)
	// fmt.Printf("GC Check File %d: valid ratio %.2f (%d/%d)\n", fid, ratio, validSize, fileSize)
	
	if ratio < gc.gcThreshold {
		// 需要回收
		// 注意：这里的回收逻辑需要与 LSM Engine 交互
		// 我们通过回调函数 checkKeyFunc 返回 true 来确认 key 有效
		// 但真正的重写逻辑需要调用 Writer.Write 并更新 LSM Tree
		// 这部分逻辑比较复杂，通常需要 LSM Engine 提供一个 "Rewrite" 接口
		// 这里我们暂时只打印日志，表示发现了可回收文件
		// fmt.Printf("GC: File %d should be compacted (ratio %.2f < %.2f)\n", fid, ratio, gc.gcThreshold)
		
		// 实际执行重写需要:
	// 1. 将 validEntries 写入 active vLog
	// 2. 更新 LSM Tree 中的索引指向新的位置
	// 3. 删除旧文件
	
	// 为了解耦，我们定义一个回调函数来执行重写操作
	if gc.rewriteFunc != nil {
		for _, entry := range validEntries {
			// 调用 rewriteFunc 将数据写入新的 vLog 并更新 LSM Tree
			err := gc.rewriteFunc(entry.key, entry.value)
			if err != nil {
				return fmt.Errorf("failed to rewrite entry during GC: %v", err)
			}
		}
		
		// 重写完成后，删除旧文件
		// 注意：这只是一个简单的删除，实际生产中可能需要更安全的删除策略（如重命名后延迟删除）
		file.Close() // 确保关闭
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("failed to remove compacted file %s: %v", filename, err)
		}
		
		return nil
	}
	
	return fmt.Errorf("GC found garbage in file %d, but rewriteFunc is not set", fid)
}
	
	return nil
}
