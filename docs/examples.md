# Gedis 使用示例

## 启动服务器

```bash
# 方式 1: 直接运行
go run cmd/server/main.go

# 方式 2: 使用 Makefile
make run
```

## 连接服务器

### 方式 1: 使用内置客户端

```bash
go run cmd/client/main.go
```

### 方式 2: 使用 redis-cli

```
redis-cli -h 127.0.0.1 -p 16379
```

## 命令示例

### 字符串操作

```bash
# 设置键值
SET name "Alice"
SET age "25"
SET city "Beijing"

# 获取键值
GET name
# 输出：Alice

GET age
# 输出：25

# 检查键是否存在
EXISTS name
# 输出：(integer) 1

EXISTS notexist
# 输出：(integer) 0

# 删除键
DEL name
# 输出：(integer) 1

# 批量删除
DEL age city
# 输出：(integer) 2

# 设置过期时间 (秒)
EXPIRE key 60
# 输出：(integer) 1

# 查询所有键
KEYS *
# 输出：1) "name"
#      2) "age"
#      3) "city"

# 查看数据库大小
DBSIZE
# 输出：(integer) 3
```

### 列表操作

```bash
# 左侧插入
LPUSH mylist "a" "b" "c"
# 输出：(integer) 3
# 列表内容：["c", "b", "a"]

# 右侧插入
RPUSH mylist "d" "e"
# 输出：(integer) 5
# 列表内容：["c", "b", "a", "d", "e"]

# 获取列表长度
LLEN mylist
# 输出：(integer) 5

# 左侧弹出
LPOP mylist
# 输出："c"
# 列表内容：["b", "a", "d", "e"]

# 右侧弹出
RPOP mylist
# 输出："e"
# 列表内容：["b", "a", "d"]

# 获取范围元素
LRANGE mylist 0 -1
# 输出：1) "b"
#      2) "a"
#      3) "d"

LRANGE mylist 0 1
# 输出：1) "b"
#      2) "a"
```

### 数据库操作

```bash
# 切换数据库
SELECT 1
# 输出：OK

# 清空当前数据库
FLUSHDB
# 输出：OK

# 清空所有数据库
# 注意：此命令会清空所有 16 个数据库
```

## 完整示例会话

```bash
$ go run cmd/server/main.go
[INFO] 2024/01/01 12:00:00 server.go:45: Gedis 服务器启动在 0.0.0.0:6379
[INFO] 2024/01/01 12:00:00 db_manager.go:30: 初始化 16 个数据库

# 新终端
$ go run cmd/client/main.go
Gedis 客户端
已连接到 127.0.0.1:6379
输入命令开始交互，输入 'exit' 退出

gedis> SET greeting "Hello, Gedis!"
响应：OK

gedis> GET greeting
响应：Hello, Gedis!

gedis> LPUSH numbers 1 2 3 4 5
响应：5

gedis> LRANGE numbers 0 2
响应：[5 4 3]

gedis> LLEN numbers
响应：5

gedis> DEL greeting
响应：1

gedis> EXISTS greeting
响应：0

gedis> exit
```

## 高级用法

### 使用 Redis 客户端库连接

#### Python (redis-py)

```python
import redis

r = redis.Redis(host='localhost', port=16379, db=0)

r.set('foo', 'bar')
print(r.get('foo'))  # 输出：b'bar'

r.lpush('mylist', 'a', 'b', 'c')
print(r.llen('mylist'))  # 输出：3
```

#### Node.js (ioredis)

```javascript
const Redis = require('ioredis');
const redis = new Redis({ port: 16379, host: 'localhost' });

await redis.set('foo', 'bar');
console.log(await redis.get('foo')); // 输出：bar

await redis.lpush('mylist', 'a', 'b', 'c');
console.log(await redis.llen('mylist')); // 输出：3
```

#### Java (Jedis)

```java
import redis.clients.jedis.Jedis;

Jedis jedis = new Jedis("localhost", 16379);
jedis.set("foo", "bar");
System.out.println(jedis.get("foo")); // 输出：bar

jedis.lpush("mylist", "a", "b", "c");
System.out.println(jedis.llen("mylist")); // 输出：3
```

## 注意事项

1. **过期时间**: EXPIRE 命令的时间单位是秒
2. **负数索引**: LRANGE 支持负数索引，-1 表示最后一个元素
3. **数据库切换**: 默认使用数据库 0，可使用 SELECT 命令切换（0-15）
4. **并发安全**: 服务器使用读写锁保证并发访问安全
5. **内存限制**: 默认最大内存 256MB，可在配置中调整
