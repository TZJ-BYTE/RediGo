# Gedis - Go 实现的 Redis 服务器

Gedis 是一个使用 Go 语言实现的 Redis 协议兼容的键值存储服务器。

## 项目结构

```
Gedis/
├── cmd/                    # 可执行文件入口
│   ├── server/            # 服务器入口
│   └── client/            # 客户端工具
├── internal/              # 内部包
│   ├── server/           # TCP 服务器实现
│   ├── protocol/         # RESP 协议解析
│   ├── database/         # 数据库核心
│   ├── datastruct/       # 数据结构定义
│   └── command/          # Redis 命令实现
├── pkg/                   # 公共包
│   └── logger/           # 日志工具
├── config/               # 配置文件
└── go.mod                # Go 模块定义
```

## 功能特性

### 已实现的数据结构
- ✅ String (字符串)
- ✅ List (列表)
- ⏳ Hash (哈希)
- ⏳ Set (集合)
- ⏳ ZSet (有序集合)

### 已实现的命令

#### 字符串命令
- `SET key value` - 设置键值
- `GET key` - 获取键值
- `DEL key [key ...]` - 删除键
- `EXISTS key [key ...]` - 检查键是否存在
- `EXPIRE key seconds` - 设置过期时间
- `KEYS pattern` - 查询匹配的键

#### 列表命令
- `LPUSH key value [value ...]` - 左侧插入
- `RPUSH key value [value ...]` - 右侧插入
- `LPOP key` - 左侧弹出
- `RPOP key` - 右侧弹出
- `LLEN key` - 获取列表长度
- `LRANGE key start stop` - 获取范围元素

#### 服务器命令
- `SELECT index` - 切换数据库
- `FLUSHDB` - 清空当前数据库
- `DBSIZE` - 获取数据库大小

## 快速开始

### 环境要求
- Go 1.21+

### 安装依赖

```bash
go mod tidy
```

### 启动服务器

```bash
go run cmd/server/main.go
```

默认监听地址：`0.0.0.0:16379`

### 使用客户端

```bash
go run cmd/client/main.go
```

或者使用 redis-cli 连接：

```bash
redis-cli -h 127.0.0.1 -p 16379
```

## 配置

可以通过修改 `config/config.go` 中的 `DefaultConfig()` 函数来自定义配置：

- `Host`: 监听地址 (默认：0.0.0.0)
- `Port`: 监听端口 (默认：6379)
- `DBCount`: 数据库数量 (默认：16)
- `MaxMemory`: 最大内存限制 (默认：256MB)
- `AOFEnabled`: 是否启用 AOF 持久化 (默认：true)
- `LogLevel`: 日志级别 (默认：info)

## 示例

```
# 启动服务器
$ go run cmd/server/main.go

# 新终端中使用客户端
$ go run cmd/client/main.go
gedis> SET name "Alice"
响应：OK
gedis> GET name
响应：Alice
gedis> LPUSH mylist a b c
响应：3
gedis> LRANGE mylist 0 -1
响应：[c b a]
```

## 使用 Redis 客户端库连接

### Python (redis-py)

```
import redis

r = redis.Redis(host='localhost', port=16379, db=0)

r.set('name', 'Alice')
print(r.get('name'))  # 输出: b'Alice'

r.lpush('mylist', 'a', 'b', 'c')
print(r.lrange('mylist', 0, -1))  # 输出: [b'c', b'b', b'a']

```

### Go (go-redis)

```
package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:16379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(context.Background(), "name", "Alice", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(context.Background(), "name").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(val) // 输出: Alice

	err = rdb.LPush(context.Background(), "mylist", "a", "b", "c").Err()
	if err != nil {
		panic(err)
	}

	vals, err := rdb.LRange(context.Background(), "mylist", 0, -1).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(vals) // 输出: [c b a]

}

```

### Node.js (ioredis)

```
const Redis = require('ioredis');
const redis = new Redis({ port: 16379, host: 'localhost' });

redis.set('name', 'Alice').then(() => {
  redis.get('name').then((val) => {
    console.log(val); // 输出: Alice

    redis.lpush('mylist', 'a', 'b', 'c').then(() => {
      redis.lrange('mylist', 0, -1).then((vals) => {
        console.log(vals); // 输出: [ 'c', 'b', 'a' ]
      });
    });
  });
});

```

### Java (Jedis)

```java
import redis.clients.jedis.Jedis;

Jedis jedis = new Jedis("localhost", 16379);

jedis.set("name", "Alice");
System.out.println(jedis.get("name"));  // 输出: Alice

jedis.lpush("mylist", "a", "b", "c");
System.out.println(jedis.lrange("mylist", 0, -1));  // 输出: [c, b, a]

```


## 开发计划

- [ ] 完善 Hash、Set、ZSet 数据结构及对应命令
- [ ] 实现 RDB 持久化
- [ ] 实现 AOF 持久化
- [ ] 添加事务支持 (MULTI/EXEC)
- [ ] 添加发布订阅功能
- [ ] 支持 Lua 脚本
- [ ] 集群模式
- [ ] 性能优化

## 技术亮点

1. **RESP 协议解析** - 完整实现 Redis 序列化协议
2. **多数据库支持** - 支持 16 个独立数据库
3. **线程安全** - 使用读写锁保证并发安全
4. **过期策略** - 支持键的过期时间设置
5. **模块化设计** - 清晰的代码结构和分层

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
