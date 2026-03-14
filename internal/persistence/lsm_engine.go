package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/TZJ-BYTE/RediGo/pkg/logger"
)

// LSMEnergy LSM 引擎
type LSMEnergy struct {
	options          *Options            // 配置选项
	mu               sync.RWMutex        // 并发控制
	
	// MemTable
	mutableMem       *MemTable           // 可变 MemTable
	immutableMem     *ImmutableMemTable  // 不可变 MemTable
	
	// WAL
	wal              *WALWriter          // WAL 写入器
	
	// SSTable
	tableCache       *TableCache
	
	nextSSTableNum   uint64              // 下一个 SSTable 编号
	
	// Version Set 和 Compaction
	versionSet       *VersionSet         // 版本集合
	compactor        *Compactor          // Compaction 执行器
	
	// 序列号
	seqNum           uint64              // 当前序列号
	
	// 目录
	dbDir            string              // 数据库目录
	walDir           string              // WAL 目录
	sstableDir       string              // SSTable 目录
	
	closed           bool                // 是否已关闭
	
	// 后台刷写同步
	flushing       atomic.Bool         // 是否正在刷写
	flushDone      chan struct{}       // 刷写完成信号
}

// OpenLSMEnergy 打开 LSM 引擎
func OpenLSMEnergy(dbDir string, options *Options) (*LSMEnergy, error) {
	engine := &LSMEnergy{
		options:        options,
		mutableMem:     NewMemTable(3), // 默认 maxLevel=3
		nextSSTableNum: 0,
		dbDir:          dbDir,
		walDir:         filepath.Join(dbDir, "wal"),
		sstableDir:     filepath.Join(dbDir, "sstable"),
		closed:         false,
		tableCache:     NewTableCache(options.MaxOpenFiles),
	}
	
	// 创建目录
	err := os.MkdirAll(dbDir, 0755)
	if err != nil {
		return nil, err
	}
	
	err = os.MkdirAll(engine.walDir, 0755)
	if err != nil {
		return nil, err
	}
	
	err = os.MkdirAll(engine.sstableDir, 0755)
	if err != nil {
		return nil, err
	}
	
	// 初始化刷写同步通道
	engine.flushDone = make(chan struct{}, 1)
	
	// 打开版本集合
	engine.versionSet, err = OpenVersionSet(dbDir, MaxLevels)
	if err != nil {
		return nil, fmt.Errorf("failed to open version set: %v", err)
	}
	
	// 从 VersionSet 恢复 nextSSTableNum
	engine.nextSSTableNum = engine.versionSet.nextFileNum
	logger.Info("Recovered nextSSTableNum: %d", engine.nextSSTableNum)
	
	// 创建 Compactor
	engine.compactor = NewCompactor(dbDir, engine.versionSet, options)
	
	// 启动后台 Compaction
	engine.compactor.Start()
	
	// SSTable 恢复不再需要加载到 engine.sstables
	// 我们将在 Get/LoadAllKeys 时按需从 VersionSet 加载
	
	logger.Info("LSM Engine initialized")
	
	// 恢复 WAL
	err = engine.recoverFromWAL()
	if err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %v", err)
	}
	
	return engine, nil
}

// recoverFromWAL 从 WAL 恢复数据
func (e *LSMEnergy) recoverFromWAL() error {
	walFile := filepath.Join(e.walDir, "current.wal")
	
	// 检查 WAL 文件是否存在
	if !WALExists(walFile) {
		// 没有 WAL 文件，创建新的
		var err error
		e.wal, err = NewWALWriter(walFile, int64(64*1024*1024)) // 默认 64MB
		if err != nil {
			return err
		}
		return nil
	}
	
	// 重放 WAL
	lastSeq, err := ReplayWAL(walFile, func(record *WALRecord) error {
		switch record.Type {
		case WALRecordTypePut:
			e.mutableMem.Put(record.Key, record.Value)
		case WALRecordTypeDelete:
			e.mutableMem.Delete(record.Key)
		}
		return nil
	})
	
	if err != nil {
		return err
	}
	
	// 更新序列号
	atomic.StoreUint64(&e.seqNum, lastSeq)
	
	// 创建新的 WAL 写入器
	e.wal, err = NewWALWriter(walFile, int64(64*1024*1024))
	if err != nil {
		return err
	}
	
	// 检查是否需要刷写 MemTable
	if int64(e.mutableMem.Size()) >= int64(4*1024*1024) { // 默认 4MB
		err = e.flushMemTable()
		if err != nil {
			return fmt.Errorf("failed to flush memtable during recovery: %v", err)
		}
	}
	
	return nil
}

// Put 写入键值对
func (e *LSMEnergy) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if e.closed {
		return fmt.Errorf("engine is closed")
	}
	
	// 1. 写入 WAL
	err := e.wal.Put(key, value)
	if err != nil {
		return fmt.Errorf("failed to write WAL: %v", err)
	}
	
	// 2. 写入 MemTable
	e.mutableMem.Put(key, value)
	
	// 3. 更新序列号
	seq := atomic.AddUint64(&e.seqNum, 1)
	_ = seq // 暂时不使用
	
	// 4. 检查是否需要刷写
	if int64(e.mutableMem.Size()) >= int64(4*1024*1024) { // 默认 4MB
		err = e.flushMemTable()
		if err != nil {
			return fmt.Errorf("failed to flush memtable: %v", err)
		}
	}
	
	return nil
}

// getSSTableReader 获取或打开 SSTable Reader
func (e *LSMEnergy) getSSTableReader(fileNum uint64) (*SSTableReader, error) {
	return e.tableCache.GetOrOpen(fileNum, func() (*SSTableReader, error) {
		sstablePath := filepath.Join(e.sstableDir, fmt.Sprintf("%06d.sstable", fileNum))
		return OpenSSTableForRead(sstablePath, e.options)
	})
}

// Get 获取值
func (e *LSMEnergy) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if e.closed {
		return nil, false
	}
	
	// 1. 先在 Mutable MemTable 中查找
	val, found := e.mutableMem.Get(key)
	if found {
		if IsDeleted(val) {
			return nil, false
		}
		return val, true
	}
	
	// 2. 在 Immutable MemTable 中查找
	if e.immutableMem != nil {
		val, found := e.immutableMem.Get(key)
		if found {
			if IsDeleted(val) {
				return nil, false
			}
			return val, true
		}
	}
	
	// 3. 在 SSTable 中查找
	// 获取当前版本
	version := e.versionSet.GetCurrentVersion()
	if version == nil {
		return nil, false
	}
	
	// 遍历 Level 0 (从新到旧)
	for i := len(version.Files[0]) - 1; i >= 0; i-- {
		fm := version.Files[0][i]
		reader, err := e.getSSTableReader(fm.FileNum)
		if err != nil {
			logger.Warn("Failed to get reader for file %d: %v", fm.FileNum, err)
			continue
		}
		
		val, found := reader.Get(key)
		if found {
			if IsDeleted(val) {
				return nil, false
			}
			return val, true
		}
	}
	
	// 遍历 Level > 0 (从低层到高层)
	for level := 1; level < MaxLevels; level++ {
		files := version.Files[level]
		if len(files) == 0 {
			continue
		}
		
		// 在该层级中查找可能包含 key 的文件
		// 由于文件是有序且不重叠的，可以通过二分查找或者检查范围
		for _, fm := range files {
			// 简单的范围检查优化
			if fm.SmallestKey != nil && fm.LargestKey != nil {
				// 如果 key < SmallestKey 或 key > LargestKey，跳过
				// 注意：这里需要正确的比较函数
				// 为简单起见，这里假设 bytes.Compare 可用，且 reader.Get 内部也会检查 BloomFilter
			}
			
			reader, err := e.getSSTableReader(fm.FileNum)
			if err != nil {
				continue
			}
			
			val, found := reader.Get(key)
			if found {
				if IsDeleted(val) {
					return nil, false
				}
				return val, true
			}
		}
	}
	
	return nil, false
}

// Delete 删除键
func (e *LSMEnergy) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if e.closed {
		return fmt.Errorf("engine is closed")
	}
	
	// 1. 写入 WAL
	err := e.wal.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to write WAL: %v", err)
	}
	
	// 2. 写入 MemTable（删除标记）
	e.mutableMem.Delete(key)
	
	// 3. 更新序列号
	seq := atomic.AddUint64(&e.seqNum, 1)
	_ = seq
	
	// 4. 检查是否需要刷写
	if int64(e.mutableMem.Size()) >= int64(4*1024*1024) { // 默认 4MB
		err = e.flushMemTable()
		if err != nil {
			return fmt.Errorf("failed to flush memtable: %v", err)
		}
	}
	
	return nil
}

// flushMemTableSync 同步刷写 MemTable 到 SSTable（用于关闭时）
func (e *LSMEnergy) flushMemTableSync() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushMemTableSyncNoLock()
}

// flushMemTableSyncNoLock 同步刷写 MemTable 到 SSTable（内部使用，假设已持有锁）
func (e *LSMEnergy) flushMemTableSyncNoLock() error {
	fmt.Printf("[FLUSH] Starting sync flush, MemTable size: %d bytes\n", e.mutableMem.Size())
	
	if e.mutableMem.Size() == 0 {
		fmt.Println("[FLUSH] MemTable is empty, skipping")
		return nil
	}
	
	// 1. 将当前 MemTable 转为 Immutable
	oldMem := e.mutableMem
	e.immutableMem = NewImmutableMemTable(oldMem)
	fmt.Printf("[FLUSH] Converted to Immutable MemTable, size: %d bytes\n", e.immutableMem.memtable.Size())
	
	// 2. 创建新的 Mutable MemTable
	e.mutableMem = NewMemTable(3)
	
	// 3. 同步刷写（不启动 goroutine）
	imm := e.immutableMem // 保存引用用于后续释放
	defer func() {
		if imm != nil {
			imm.Unref()
			fmt.Println("[FLUSH] Unref immutable memtable")
		}
	}()
	
	fmt.Println("[FLUSH] Calling flushImmutableToSSTable...")
	err := e.flushImmutableToSSTable(imm)
	if err != nil {
		fmt.Printf("[FLUSH] ERROR: Failed to flush immutable memtable: %v\n", err)
		return fmt.Errorf("failed to flush immutable memtable: %v", err)
	}
	
	fmt.Println("[FLUSH] Flush completed successfully")
	e.immutableMem = nil
	return nil
}

// flushMemTable 异步刷写 MemTable 到 SSTable
func (e *LSMEnergy) flushMemTable() error {
	if e.mutableMem.Size() == 0 {
		return nil
	}
	
	// 1. 将当前 MemTable 转为 Immutable
	oldMem := e.mutableMem
	e.immutableMem = NewImmutableMemTable(oldMem)
	
	// 2. 创建新的 Mutable MemTable
	e.mutableMem = NewMemTable(3) // 默认 maxLevel=3
	
	// 3. 异步刷写 Immutable MemTable 到 SSTable
	go func(imm *ImmutableMemTable) {
		defer func() {
			imm.Unref() // 减少引用计数
			// 发送刷写完成信号
			select {
			case e.flushDone <- struct{}{}:
				// 信号发送成功
			default:
				// 通道已满，不阻塞
			}
			e.flushing.Store(false)
		}()
		
		err := e.flushImmutableToSSTable(imm)
		if err != nil {
			fmt.Printf("Error flushing immutable memtable: %v\n", err)
		}
	}(e.immutableMem)
	
	e.immutableMem = nil
	e.flushing.Store(true)
	
	return nil
}

// flushImmutableToSSTable 将 Immutable MemTable 刷写到 SSTable
func (e *LSMEnergy) flushImmutableToSSTable(imm *ImmutableMemTable) error {
	fmt.Println("[SSTABLE] Starting flush to SSTable...")
	
	// 生成 SSTable 文件名
	sstableNum := e.versionSet.GetNextFileNum()
	filename := filepath.Join(e.sstableDir, fmt.Sprintf("%06d.sstable", sstableNum))
	fmt.Printf("[SSTABLE] Will create SSTable file: %s (num=%d)\n", filename, sstableNum)
	
	// 创建 SSTable Builder
	builder, err := NewSSTableBuilder(filename, e.options)
	if err != nil {
		return fmt.Errorf("failed to create sstable builder: %v", err)
	}
	defer builder.Abort() // 如果出错则回滚
	
	fmt.Println("[SSTABLE] Iterating immutable memtable...")
	entryCount := 0
	err = imm.ForEach(func(key, value []byte) error {
		entryCount++
		fmt.Printf("[SSTABLE] Adding entry #%d: key=%s, value_size=%d\n", entryCount, string(key), len(value))
		return builder.Add(key, value)
	})
	if err != nil {
		return fmt.Errorf("failed to iterate immutable memtable: %v", err)
	}
	
	fmt.Printf("[SSTABLE] Finished iteration, added %d entries\n", entryCount)
	
	// 完成 SSTable 构建
	fmt.Println("[SSTABLE] Finishing SSTable build...")
	err = builder.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish sstable: %v", err)
	}
	fmt.Println("[SSTABLE] SSTable build completed")
	
	sstablePath := filepath.Join(e.sstableDir, fmt.Sprintf("%06d.sstable", sstableNum))
	reader, err := OpenSSTableForRead(sstablePath, e.options)
	if err != nil {
		return fmt.Errorf("failed to open sstable reader: %v", err)
	}
	
	// 获取文件信息
	info, err := os.Stat(filename)
	if err != nil {
		reader.Close()
		return fmt.Errorf("failed to stat sstable file: %v", err)
	}
	fmt.Printf("[SSTABLE] SSTable file size: %d bytes\n", info.Size())
	
	// 创建文件元数据
	fm := &FileMetadata{
		FileNum:     sstableNum,
		Size:        info.Size(),
		SmallestKey: nil, // TODO: 从 builder 获取
		LargestKey:  nil, // TODO: 从 builder 获取
		Level:       0,   // Level 0
	}
	
	// 添加到版本集合
	fmt.Println("[SSTABLE] Adding file to version set...")
	err = e.versionSet.LogAddFile(fm)
	if err != nil {
		reader.Close()
		return fmt.Errorf("failed to log add file: %v", err)
	}
	fmt.Println("[SSTABLE] File added to version set successfully")
	
	// 添加到缓存
	e.tableCache.Add(sstableNum, reader)
	fmt.Printf("[SSTABLE] Added to table cache\n")
	
	return nil
}

// Close 关闭 LSM 引擎
func (e *LSMEnergy) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if e.closed {
		return nil
	}
	
	e.closed = true
	
	// 创建关闭标记文件（用于调试）
	os.WriteFile("/tmp/lsm_close_called.txt", []byte("Close called at "+time.Now().String()), 0644)
	
	logger.Info("=== CLOSING LSM ENGINE ===")
	logger.Info("MemTable size before close: %d bytes", e.mutableMem.Size())
	
	// 1. 停止后台 Compaction
	if e.compactor != nil {
		e.compactor.Stop()
		logger.Info("Stopped compactor")
	}
	
	// 2. 同步刷写当前 MemTable（强制刷写，无论大小）
	// 注意：需要在持有锁的情况下调用，但 flushMemTableSync 不需要额外获取锁
	logger.Info("Forcing flush of MemTable with size %d bytes...", e.mutableMem.Size())
	err := e.flushMemTableSyncNoLock()
	if err != nil {
		logger.Error("Error flushing memtable: %v", err)
		return err
	}
	logger.Info("MemTable flushed successfully (forced)")
	
	// 3. 关闭 WAL
	if e.wal != nil {
		err := e.wal.Close()
		if err != nil {
			// 在 Windows 上，如果文件仍然被占用（尽管我们已经尝试关闭了），Close 可能会失败
			// 但我们仍然应该继续关闭其他资源
			logger.Warn("Failed to close WAL: %v", err)
		} else {
			logger.Info("WAL closed")
		}
	}
	
	// 4. 关闭所有 SSTable
	e.tableCache.Close()
	logger.Info("Closed table cache")
	
	// 5. 关闭版本集合
	if e.versionSet != nil {
		err := e.versionSet.Close()
		if err != nil {
			return err
		}
		logger.Info("VersionSet closed")
	}
	
	fmt.Println("LSM Engine closed successfully")
	return nil
}

// GetSeqNum 获取当前序列号
func (e *LSMEnergy) GetSeqNum() uint64 {
	return atomic.LoadUint64(&e.seqNum)
}

// GetSSTableCount 获取 SSTable 数量
func (e *LSMEnergy) GetSSTableCount() int {
	return e.versionSet.GetCurrentVersion().GetFileCount()
}

// LoadAllKeys 加载所有 key-value 到内存（用于冷启动全量加载）
// 返回一个 map[string][]byte，包含所有未删除的键值对
func (e *LSMEnergy) LoadAllKeys() (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	result := make(map[string][]byte)
	deletedKeys := make(map[string]struct{}) // 记录已删除的 key
	
	fmt.Printf("[LoadAllKeys] Starting to load keys...\n")
	
	// 辅助函数：处理单个 key-value
	processEntry := func(source string, key, value []byte) {
		keyStr := string(key)
		
		// DEBUG: 打印每个找到的 key
		if len(result) < 5 {
			fmt.Printf("[LoadAllKeys] Processing key: %s (source: %s, val_len: %d, deleted: %v)\n", keyStr, source, len(value), IsDeleted(value))
		}
		
		// 如果已经被标记为删除，或者已经加载了较新版本，则跳过
		if _, deleted := deletedKeys[keyStr]; deleted {
			return
		}
		if _, exists := result[keyStr]; exists {
			return
		}
		
		// 检查是否为 Tombstone
		if IsDeleted(value) {
			// fmt.Printf("[LoadAllKeys] Found tombstone for key: %s in %s\n", keyStr, source)
			deletedKeys[keyStr] = struct{}{}
		} else {
			// 加载数据
			// fmt.Printf("[LoadAllKeys] Loaded key: %s from %s, value_size=%d\n", keyStr, source, len(value))
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result[keyStr] = valueCopy
		}
	}
	
	// 1. 从 MemTable 加载
	if e.mutableMem != nil {
		it := e.mutableMem.Iterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			processEntry("MemTable", it.Key(), it.Value())
		}
	}
	
	// 2. 从 Immutable MemTable 加载
	if e.immutableMem != nil && e.immutableMem.memtable != nil {
		it := e.immutableMem.memtable.Iterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			processEntry("ImmutableMemTable", it.Key(), it.Value())
		}
	}
	
	// 3. 从所有 SSTable 加载
	// 注意：LoadAllKeys 需要按照数据的新旧顺序（从新到旧）来加载，以确保旧数据不会覆盖新数据。
	// 但 processEntry 内部已经有了 exists 检查：`if _, exists := result[keyStr]; exists { return }`
	// 所以只要我们保证“新数据先被处理”，逻辑就是正确的。
	
	version := e.versionSet.GetCurrentVersion()
	if version != nil {
		// Level 0: 文件是重叠的，且按 FileNum 排序（通常较新的在后面，但 Compaction 可能改变这一点）。
		// 在 Level 0 中，FileNum 越大通常意味着越新（由 Immutable MemTable 刷写产生）。
		// 所以我们需要从大到小遍历 Level 0 文件。
		for i := len(version.Files[0]) - 1; i >= 0; i-- {
			fm := version.Files[0][i]
			// 修复：确保 FileNum 正确
			if fm == nil {
				continue
			}
			
			reader, err := e.getSSTableReader(fm.FileNum)
			if err != nil {
				logger.Warn("Failed to get reader for file %d: %v", fm.FileNum, err)
				continue
			}
			
			it := reader.NewIterator()
			// 修复：检查 iterator 是否有效
			if it == nil {
				logger.Warn("Failed to create iterator for file %d", fm.FileNum)
				continue
			}
			
			iterCount := 0
			for it.SeekToFirst(); it.Valid(); it.Next() {
				processEntry(fmt.Sprintf("SSTable#%d(L0)", fm.FileNum), it.Key(), it.Value())
				iterCount++
			}
			it.Release()
			
			// DEBUG
			// fmt.Printf("[LoadAllKeys] Scanned SSTable %d (L0), found %d entries\n", fm.FileNum, iterCount)
		}
		
		// Level > 0: 文件不重叠，且层级越小数据越新。
		// 所以我们需要先处理 Level 1，然后 Level 2...
		// 在同一层级内，文件是不重叠的，所以顺序不影响正确性（对于不同的 Key）。
		// 但为了保险起见，我们还是顺序遍历。
		for level := 1; level < MaxLevels; level++ {
			for _, fm := range version.Files[level] {
				reader, err := e.getSSTableReader(fm.FileNum)
				if err != nil {
					continue
				}
				
				it := reader.NewIterator()
				for it.SeekToFirst(); it.Valid(); it.Next() {
					processEntry(fmt.Sprintf("SSTable#%d(L%d)", fm.FileNum, level), it.Key(), it.Value())
				}
				it.Release()
			}
		}
	}
	
	fmt.Printf("[LoadAllKeys] Loaded total %d keys, ignored %d deleted keys\n", len(result), len(deletedKeys))
	return result, nil
}
