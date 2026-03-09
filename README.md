# RediGo - Go 实现的 Redis 服务器

RediGo 是一个使用 Go 语言实现的 Redis 协议兼容的键值存储服务器。

## 项目结构

```
RediGo/
├── cmd/                    # 可执行文件入口
│   ├── server/            # 服务器入口
│   └── client/            # 客户端工具
├── internal/              # 内部包
│   ├── server/           # TCP 服务器实现
│   ├── protocol/         # RESP协议解析
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
- ✅ Hash (哈希)
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

#### 哈希命令 ✨
- `HSET key field value` - 设置哈希字段值
- `HGET key field` - 获取哈希字段值
- `HMSET key field value [field value ...]` - 批量设置哈希字段值
- `HMGET key field [field ...]` - 批量获取哈希字段值
- `HDEL key field [field ...]` - 删除哈希字段
- `HLEN key` - 获取哈希大小
- `HEXISTS key field` - 检查字段是否存在
- `HKEYS key` - 获取所有字段名
- `HVALS key` - 获取所有字段值
- `HGETALL key` - 获取所有字段和值
- `HINCRBY key field increment` - 递增整数字段
- `HINCRBYFLOAT key field increment` - 递增浮点数字段

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

# Hash 操作示例
gedis> HSET user:1 name "张三"
响应：1
gedis> HSET user:1 age "25"
响应：1
gedis> HGET user:1 name
响应："张三"
gedis> HMSET user:1 city "北京" country "中国"
响应：OK
gedis> HGETALL user:1
响应：["name" "张三" "age" "25" "city" "北京" "country" "中国"]
gedis> HLEN user:1
响应：4
```

## 使用 Redis 客户端库连接

### Python (redis-py)

```python
import redis

r = redis.Redis(host='localhost', port=16379, db=0)

r.set('name', 'Alice')
print(r.get('name'))  # 输出：b'Alice'

r.lpush('mylist', 'a', 'b', 'c')
print(r.lrange('mylist', 0, -1))  # 输出：[b'c', b'b', b'a']

# Hash 操作
r.hset('user:1', 'name', '张三')
r.hset('user:1', 'age', '25')
print(r.hget('user:1', 'name'))  # 输出：b'张三'
print(r.hgetall('user:1'))  # 输出：{b'name': b'张三', b'age': b'25'}

```

### Go (go-redis)

```go
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
	fmt.Println(val) // 输出：Alice

	err = rdb.LPush(context.Background(), "mylist", "a", "b", "c").Err()
	if err != nil {
		panic(err)
	}

	vals, err := rdb.LRange(context.Background(), "mylist", 0, -1).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(vals) // 输出：[c b a]

	// Hash 操作
	err = rdb.HSet(context.Background(), "user:1", "name", "张三").Err()
	if err != nil {
		panic(err)
	}
	err = rdb.HSet(context.Background(), "user:1", "age", "25").Err()
	if err != nil {
		panic(err)
	}
	
	val, err = rdb.HGet(context.Background(), "user:1", "name").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(val) // 输出：张三

	vals_map, err := rdb.HGetAll(context.Background(), "user:1").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(vals_map) // 输出：map[name:张三 age:25]

}

```

### Node.js (ioredis)

```
const Redis = require('ioredis');
const redis = new Redis({ port: 16379, host: 'localhost' });

redis.set('name', 'Alice').then(() => {
  redis.get('name').then((val) => {
    console.log(val); // 输出：Alice

    redis.lpush('mylist', 'a', 'b', 'c').then(() => {
      redis.lrange('mylist', 0, -1).then((vals) => {
        console.log(vals); // 输出：[ 'c', 'b', 'a' ]
        
        // Hash 操作
        redis.hset('user:1', 'name', '张三', 'age', '25').then(() => {
          redis.hget('user:1', 'name').then((val) => {
            console.log(val); // 输出：张三
            redis.hgetall('user:1').then((map) => {
              console.log(map); // 输出：{ name: '张三', age: '25' }
            });
          });
        });
      });
    });
  });
});

```

### Java (Jedis)

```
import redis.clients.jedis.Jedis;
import java.util.Map;

Jedis jedis = new Jedis("localhost", 16379);

jedis.set("name", "Alice");
System.out.println(jedis.get("name"));  // 输出：Alice

jedis.lpush("mylist", "a", "b", "c");
System.out.println(jedis.lrange("mylist", 0, -1));  // 输出：[c, b, a]

// Hash 操作
jedis.hset("user:1", "name", "张三");
jedis.hset("user:1", "age", "25");
System.out.println(jedis.hget("user:1", "name"));  // 输出：张三
Map<String, String> userMap = jedis.hgetAll("user:1");
System.out.println(userMap);  // 输出：{name=张三，age=25}

```


## 开发计划

- ✅ 完善 Hash 数据结构及命令实现
- ⏳ 完善 Set、ZSet 数据结构及对应命令
- [ ] 实现 RDB 持久化
- [ ] 实现 AOF 持久化
- [ ] 添加事务支持 (MULTI/EXEC)
- [ ] 添加发布订阅功能
- [ ] 支持 Lua 脚本
- [ ] 集群模式
- [ ] 性能优化

## 技术亮点

1. **RESP协议解析** - 完整实现 Redis 序列化协议
2. **多数据库支持** - 支持 16 个独立数据库
3. **线程安全** - 使用读写锁保证并发安全
4. **过期策略** - 支持键的过期时间设置
5. **模块化设计** - 清晰的代码结构和分层
6. **Hash 支持** - 完整的哈希表操作和字段管理

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
