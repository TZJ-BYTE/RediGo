# Gedis 项目总览

## 🎯 项目目标

Gedis 是一个使用 Go 语言实现的 **Redis 协议兼容**的键值存储服务器，旨在：
- 学习 Redis 的工作原理
- 掌握 Go 语言的网络编程
- 理解分布式系统的基础组件
- 提供一个可扩展的教学项目

## 📦 技术栈

| 组件 | 技术 | 说明 |
|------|------|------|
| 语言 | Go 1.21+ | 高性能、并发友好 |
| 协议 | RESP | Redis 序列化协议 |
| 网络 | TCP | 基于 net 包 |
| 并发 | Goroutine | 每连接一个 goroutine |
| 同步 | sync.RWMutex | 读写锁保护数据 |

## 🏗️ 架构分层

```
┌─────────────────────────────────────┐
│         Application Layer            │
│      (cmd/server, cmd/client)        │
├─────────────────────────────────────┤
│         Network Layer                │
│      (internal/server)               │
│   - TCP Server                       │
│   - Connection Management            │
├─────────────────────────────────────┤
│         Protocol Layer               │
│      (internal/protocol)             │
│   - RESP Parser                      │
│   - Request/Response Encoding        │
├─────────────────────────────────────┤
│         Command Layer                │
│      (internal/command)              │
│   - Command Interface                │
│   - Command Implementations          │
│   - Command Registry                 │
├─────────────────────────────────────┤
│         Storage Layer                │
│      (internal/database +            │
│       internal/datastruct)           │
│   - Database Instance                │
│   - Data Structures                  │
│   - Concurrency Control              │
├─────────────────────────────────────┤
│         Support Layer                │
│      (pkg/logger, config)            │
│   - Logging                          │
│   - Configuration                    │
└─────────────────────────────────────┘
```

## 🔄 请求处理流程

```
客户端请求
    ↓
TCP 监听 (net.Listener)
    ↓
接受连接 (Accept)
    ↓
创建 Goroutine 处理
    ↓
RESP 解析 (Parser.ParseRequest)
    ↓
命令查找 (Registry.Get)
    ↓
命令执行 (Command.Execute)
    ↓
数据库操作 (Database.Get/Set)
    ↓
RESP 编码 (EncodeResponse)
    ↓
返回响应 (conn.Write)
```

## 📊 数据结构关系

```
DataValue (带过期时间)
    ├── String (字符串)
    ├── List (列表)
    │   ├── PushLeft/Right
    │   └── PopLeft/Right
    ├── Hash (哈希)
    │   ├── Set/Get
    │   └── Delete
    ├── Set (集合)
    │   ├── Add/Remove
    │   └── Contains
    └── ZSet (有序集合)
        ├── Add/Remove
        └── Score/Range
```

## 🔐 并发安全模型

```
┌──────────────────────────────────┐
│     Goroutine 1 (Client A)       │
│           ↓                      │
│     ┌─────────────────┐          │
│     │  RWMutex Lock   │          │
│     │  (per Database) │          │
│     └─────────────────┘          │
│           ↓                      │
│     Database Access              │
└──────────────────────────────────┘

┌──────────────────────────────────┐
│     Goroutine 2 (Client B)       │
│           ↓                      │
│     ┌─────────────────┐          │
│     │  RWMutex Lock   │          │
│     │  (shared)       │          │
│     └─────────────────┘          │
│           ↓                      │
│     Database Access              │
└──────────────────────────────────┘
```

## 📁 完整目录结构

```
/home/tzj/GoLang/Gedis/
│
├── README.md                        # 项目主文档
├── Makefile                         # 构建脚本
├── .gitignore                       # Git 忽略文件
├── go.mod                           # Go 模块定义
│
├── cmd/
│   ├── server/
│   │   └── main.go                 # 服务器入口 (80 行)
│   └── client/
│       └── main.go                 # 客户端入口 (90 行)
│
├── internal/
│   ├── server/
│   │   └── server.go              # TCP 服务器 (150 行)
│   │
│   ├── protocol/
│   │   └── parser.go              # RESP 解析器 (250 行)
│   │
│   ├── database/
│   │   ├── database.go            # 数据库实例 (120 行)
│   │   └── db_manager.go          # 数据库管理器 (90 行)
│   │
│   ├── datastruct/
│   │   └── data.go                # 数据结构 (300 行)
│   │
│   └── command/
│       ├── interface.go           # 命令接口 (20 行)
│       ├── basic.go               # 基础命令 (180 行)
│       ├── list.go                # 列表命令 (150 行)
│       ├── registry.go            # 命令注册表 (80 行)
│       └── basic_test.go          # 单元测试 (120 行)
│
├── pkg/
│   └── logger/
│       └── logger.go              # 日志工具 (70 行)
│
├── config/
│   └── config.go                  # 配置定义 (40 行)
│
├── docs/
│   ├── architecture.md            # 架构文档
│   ├── examples.md                # 使用示例
│   ├── PROJECT_SUMMARY.md         # 项目总结
│   └── QUICK_REFERENCE.md         # 快速参考
│
└── scripts/
    └── start.sh                   # 启动脚本
```

## 📈 代码统计

| 类别 | 文件数 | 代码行数 |
|------|--------|----------|
| 服务器核心 | 4 | ~690 行 |
| 协议解析 | 1 | ~250 行 |
| 数据存储 | 3 | ~510 行 |
| 命令实现 | 5 | ~550 行 |
| 工具组件 | 2 | ~110 行 |
| 测试代码 | 1 | ~120 行 |
| **总计** | **16** | **~2230 行** |

## 🎓 核心知识点

### Go 语言特性
- ✅ Struct 和 Interface
- ✅ Goroutine 和 Channel
- ✅ sync.RWMutex
- ✅ bufio 缓冲 IO
- ✅ net 网络编程
- ✅ Context 上下文
- ✅ Defer 延迟执行

### 系统设计
- ✅ TCP 服务器设计
- ✅ 协议解析
- ✅ 并发控制
- ✅ 数据持久化（待实现）
- ✅ 内存管理
- ✅ 错误处理

### Redis 原理
- ✅ RESP 协议
- ✅ 命令系统
- ✅ 数据结构
- ✅ 过期策略
- ✅ 多数据库
- ⏳ 持久化机制
- ⏳ 复制集群

## 🚀 运行效果

### 服务器启动
```bash
$ make run
[INFO] 2024/01/01 12:00:00 正在启动 Gedis 服务器...
[INFO] 2024/01/01 12:00:00 配置：Host=0.0.0.0, Port=6379, DBCount=16
[INFO] 2024/01/01 12:00:00 初始化 16 个数据库
[INFO] 2024/01/01 12:00:00 Gedis 服务器启动在 0.0.0.0:6379
```

### 客户端交互
```bash
$ make client
gedis> SET name Alice
响应：OK
gedis> GET name
响应：Alice
gedis> LPUSH list a b c
响应：3
gedis> LRANGE list 0 -1
响应：[c b a]
gedis> DBSIZE
响应：2
```

## 🎁 项目特色

1. **教学友好**: 代码清晰，注释详细
2. **易于扩展**: 模块化设计，添加功能简单
3. **生产级代码**: 错误处理完善，线程安全
4. **文档齐全**: 多个文档从不同角度讲解
5. **即开即用**: 一条命令即可启动

## 🔮 未来规划

### Phase 1 - 完善基础 (当前)
- [x] 项目框架
- [x] 基础命令
- [x] 列表命令
- [ ] Hash/Set/ZSet 命令
- [ ] 更多字符串命令

### Phase 2 - 持久化
- [ ] AOF 日志
- [ ] RDB 快照
- [ ] 混合持久化
- [ ] 重启恢复

### Phase 3 - 高级特性
- [ ] 事务 (MULTI/EXEC)
- [ ] 发布订阅
- [ ] Lua 脚本
- [ ] 管道 (Pipeline)

### Phase 4 - 分布式
- [ ] 主从复制
- [ ] 哨兵模式
- [ ] 集群分片
- [ ] 一致性协议

## 📚 学习资源

### 推荐阅读顺序
1. 📖 [README.md](../README.md) - 了解项目概况
2. 📖 [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - 快速上手
3. 📖 [examples.md](examples.md) - 查看使用示例
4. 📖 [architecture.md](architecture.md) - 深入架构设计
5. 📖 [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) - 完整总结

### 代码阅读路径
1. `cmd/server/main.go` - 程序入口
2. `internal/server/server.go` - 服务器核心
3. `internal/protocol/parser.go` - 协议解析
4. `internal/command/basic.go` - 命令实现
5. `internal/database/database.go` - 数据存储

## 🤝 参与贡献

欢迎通过以下方式贡献：
- 🐛 提交 Bug 报告
- 💡 提出新功能建议
- 📝 改进文档
- 🔧 提交代码优化

---

**项目状态**: ✅ 基础版本完成  
**维护者**: Gedis Team  
**许可证**: MIT License
