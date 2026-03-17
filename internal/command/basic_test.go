package command_test

import (
	"testing"
	
	"github.com/TZJ-BYTE/RediGo/config"
	"github.com/TZJ-BYTE/RediGo/internal/command"
	"github.com/TZJ-BYTE/RediGo/internal/datastruct"
	"github.com/TZJ-BYTE/RediGo/internal/database"
)

func TestSetAndGet(t *testing.T) {
	cfg := config.DefaultConfig()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 测试 SET
	setCmd := &command.SetCommand{}
	resp := setCmd.Execute(db, [][]byte{[]byte("name"), []byte("Alice")})
	if resp.Type != '+' {
		t.Errorf("SET 命令返回错误类型：%v", resp.Type)
	}
	
	// 测试 GET
	getCmd := &command.GetCommand{}
	resp = getCmd.Execute(db, [][]byte{[]byte("name")})
	if resp.Type != '$' {
		t.Errorf("GET 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != "Alice" {
		t.Errorf("GET 返回值错误，期望：Alice, 实际：%v", resp.Value)
	}
}

func TestDel(t *testing.T) {
	cfg := config.DefaultConfig()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 先设置一个值
	db.Set("testkey", &datastruct.DataValue{
		Value:      &datastruct.String{Data: "testvalue"},
		ExpireTime: 0,
	})
	
	// 测试 DEL
	delCmd := &command.DelCommand{}
	resp := delCmd.Execute(db, [][]byte{[]byte("testkey")})
	if resp.Type != ':' {
		t.Errorf("DEL 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(1) {
		t.Errorf("DEL 返回值错误，期望：1, 实际：%v", resp.Value)
	}
}

func TestExists(t *testing.T) {
	cfg := config.DefaultConfig()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 先设置一个值
	db.Set("existkey", &datastruct.DataValue{
		Value:      &datastruct.String{Data: "existvalue"},
		ExpireTime: 0,
	})
	
	// 测试 EXISTS
	existsCmd := &command.ExistsCommand{}
	resp := existsCmd.Execute(db, [][]byte{[]byte("existkey")})
	if resp.Type != ':' {
		t.Errorf("EXISTS 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(1) {
		t.Errorf("EXISTS 返回值错误，期望：1, 实际：%v", resp.Value)
	}
}

func TestLPushAndLPop(t *testing.T) {
	cfg := config.DefaultConfig()
	dbManager := database.NewDBManager(cfg)
	db := dbManager.GetDefaultDB()

	// 测试 LPUSH
	lpushCmd := &command.LPushCommand{}
	resp := lpushCmd.Execute(db, [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")})
	if resp.Type != ':' {
		t.Errorf("LPUSH 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != int64(3) {
		t.Errorf("LPUSH 返回值错误，期望：3, 实际：%v", resp.Value)
	}
	
	// 测试 LPOP
	lpopCmd := &command.LPopCommand{}
	resp = lpopCmd.Execute(db, [][]byte{[]byte("mylist")})
	if resp.Type != '$' {
		t.Errorf("LPOP 命令返回错误类型：%v", resp.Type)
	}
	if resp.Value != "c" {
		t.Errorf("LPOP 返回值错误，期望：c, 实际：%v", resp.Value)
	}
}
