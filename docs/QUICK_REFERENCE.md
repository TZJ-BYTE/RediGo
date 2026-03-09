# Gedis 快速参考

## 项目结构概览

```
Gedis/
├── cmd/           # 可执行文件
│   ├── server/   # 服务器入口
│   └── client/   # 客户端入口
├── internal/      # 内部实现
│   ├── server/   # TCP 服务器
│   ├── protocol/ # RESP 协议
│   ├── database/ # 数据库核心
│   ├── datastruct/# 数据结构
│   └── command/  # Redis 命令
├── pkg/          # 公共组件
│   └── logger/   # 日志工具
├── config/       # 配置
├── docs/         # 文档
├── scripts/      # 脚本
└── Makefile      # 构建脚本
```

## 快速命令

### 启动服务器
```bash
make run              # 使用 Makefile
go run cmd/server/main.go  # 直接运行
./scripts/start.sh    # 使用启动脚本
```

### 连接客户端
```
# 使用内置客户端
$ make client

# 或使用 redis-cli
$ redis-cli -h 127.0.0.1 -p 16379
```

### 构建和测试
```bash
make build           # 构建所有
make test            # 运行测试
make clean           # 清理构建
make fmt             # 格式化代码
```

## 命令示例

### 字符串
```
SET key value
GET key
DEL key [key ...]
EXISTS key [key ...]
EXPIRE key seconds
KEYS pattern
```

### 列表
```
LPUSH key value [value ...]
RPUSH key value [value ...]
LPOP key
RPOP key
LLEN key
LRANGE key start stop
```

### 数据库
```
SELECT index
FLUSHDB
DBSIZE
```

## 响应类型

- `+` 简单字符串 (OK)
- `-` 错误 (ERR ...)
- `:` 整数 (123)
- `$` 批量字符串 ("hello")
- `*` 数组 ([a, b, c])

## 代码示例

### 添加新命令
```go
// 1. 创建命令结构
type MyCommand struct{}

func (c *MyCommand) Execute(db *database.Database, args []string) *protocol.Response {
    // 实现逻辑
    return protocol.MakeSimpleString("OK")
}

// 2. 注册命令
DefaultRegistry.Register("MYCMD", &MyCommand{})
```

### 使用客户端库

**Python:**
```python
import redis
r = redis.Redis(host='localhost', port=16379)
r.set('foo', 'bar')
print(r.get('foo'))
```

**Node.js:**
```javascript
const redis = require('ioredis');
const r = new Redis({ port: 16379, host: 'localhost' });
await r.set('foo', 'bar');
console.log(await r.get('foo'));
```

## 配置文件

编辑 `config/config.go`:
```
func DefaultConfig() *Config {
    return &Config{
        Host:      "0.0.0.0",
        Port:      16380,  # 修改 config/config.go 中的 Port
        DBCount:   16,
        MaxMemory: 256 * 1024 * 1024,
        AOFEnabled: true,
        LogLevel:  "info",
    }
}
```

## 调试技巧

1. **查看日志**: `tail -f logs/gedis.log`
2. **启用调试日志**: 修改 LogLevel 为 "debug"
3. **测试连接**: `telnet 127.0.0.1 6379`
4. **监控状态**: 使用 INFO 命令（待实现）

## 常见问题

**Q: 端口被占用？**
A: 修改 config.go 中的 Port 配置

**Q: 如何清空数据？**
A: 使用 FLUSHDB 或 FLUSHALL 命令

**Q: 数据持久化？**
A: 待实现 AOF/RDB，目前重启后数据丢失

**Q: 最大连接数？**
A: 默认无限制，由操作系统决定

## 性能提示

1. 使用管道减少网络往返
2. 避免大 key 和长列表
3. 合理设置过期时间
4. 定期清理无用数据

## 开发清单

- [ ] Hash 命令
- [ ] Set 命令  
- [ ] ZSet 命令
- [ ] AOF 持久化
- [ ] RDB 快照
- [ ] 事务支持
- [ ] 发布订阅
- [ ] Lua 脚本

## 更多信息

- 📖 [完整文档](README.md)
- 🏗️ [架构说明](docs/architecture.md)
- 💡 [使用示例](docs/examples.md)
- 📊 [项目总结](docs/PROJECT_SUMMARY.md)

---
**版本**: 1.0.0  
**Go 版本**: 1.21+  
**许可证**: MIT
