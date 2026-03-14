package persistence

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// DataValue 定义在这里是为了测试，模拟 datastruct.DataValue
// 因为我们无法在同一个测试包中引用 internal/datastruct
type TestDataValue struct {
	Value      string
	ExpireTime int64
}

// 模拟 Serialize
func (dv *TestDataValue) Serialize() ([]byte, error) {
	// 简单的模拟实现
	return append([]byte{0x01}, []byte(dv.Value)...), nil
}

// 模拟 Deserialize
func DeserializeTestDataValue(data []byte) (*TestDataValue, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty")
	}
	if data[0] == 0x01 {
		return &TestDataValue{Value: string(data[1:])}, nil
	}
	return nil, fmt.Errorf("invalid prefix")
}

// TestMemTableDeleteTombstone 验证 MemTable 删除操作是否插入 Tombstone
func TestMemTableDeleteTombstone(t *testing.T) {
	mt := NewMemTable(1024 * 1024)
	key := []byte("key_to_delete")
	value := []byte("value")
	
	// 1. 写入数据
	mt.Put(key, value)
	
	val, found := mt.Get(key)
	if !found || string(val) != string(value) {
		t.Fatalf("Put failed")
	}
	
	// 2. 删除数据
	mt.Delete(key)
	
	// 3. 验证 Get 不再返回数据 (或返回 Tombstone)
	val, found = mt.Get(key)
	if found {
		if IsDeleted(val) {
			t.Log("MemTable.Get returned Tombstone, which confirms insertion")
		} else {
			t.Errorf("MemTable.Get returned non-Tombstone value after delete: %v", val)
		}
	} else {
		t.Log("MemTable.Get returned false, which is also acceptable if logic handles it")
	}
	
	// 4. 验证迭代器是否能看到 Tombstone
	it := mt.Iterator()
	it.Seek(key)
	if !it.Valid() {
		t.Error("Iterator should find the key (even if it is a tombstone)")
	} else {
		if string(it.Key()) != string(key) {
			t.Errorf("Iterator found wrong key: %s", string(it.Key()))
		}
		if !IsDeleted(it.Value()) {
			t.Errorf("Iterator value should be Tombstone, got: %v", it.Value())
		}
	}
}

// TestLSMEngineTombstoneIntegration 验证 LSM 引擎对 Tombstone 的处理（防止数据复活）
func TestLSMEngineTombstoneIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lsm_fix_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	opts := DefaultOptions()
	opts.MemTableSize = 1024 // 小一点，方便触发刷写
	
	engine, err := OpenLSMEnergy(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	
	key := []byte("resurrect_key")
	value1 := []byte("version_1")
	
	// 1. 写入版本 1
	engine.Put(key, value1)
	
	// 2. 强制刷写到 SSTable（通过关闭引擎或手动触发）
	// 这里我们使用内部方法强制刷写（如果可以访问）或者写入足够多数据
	// 为了简单，我们先写入大量无关数据触发刷写
	for i := 0; i < 1000; i++ {
		engine.Put([]byte(fmt.Sprintf("padding_%d", i)), make([]byte, 100))
	}
	
	// 等待后台刷写完成（简单 sleep）
	time.Sleep(100 * time.Millisecond)
	
	// 3. 删除 key
	engine.Delete(key)
	
	// 4. 再次写入大量数据，确保删除标记也被刷写到新的 SSTable
	for i := 0; i < 1000; i++ {
		engine.Put([]byte(fmt.Sprintf("padding_2_%d", i)), make([]byte, 100))
	}
	time.Sleep(100 * time.Millisecond)
	
	// 5. 验证 Get 不应该返回数据
	val, found := engine.Get(key)
	if found {
		if IsDeleted(val) {
			// 如果 Get 返回 Tombstone，这在某些实现中是允许的，但在对外接口中应该过滤
		} else {
			t.Errorf("Key should be deleted, but got value: %s", string(val))
		}
	}
	
	// 6. 验证 LoadAllKeys（模拟重启）
	// 关闭并重新打开
	engine.Close()
	
	engine2, err := OpenLSMEnergy(tmpDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()
	
	loadedData, err := engine2.LoadAllKeys()
	if err != nil {
		t.Fatal(err)
	}
	
	if val, exists := loadedData[string(key)]; exists {
		t.Errorf("Deleted key resurrected after reload! Value: %s", string(val))
	} else {
		t.Log("Success: Deleted key did not resurrect.")
	}
}
