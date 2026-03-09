# Gedis 项目结构说明

## 目录结构

```
Gedis/
├── cmd/                          # 可执行文件入口目录
│   ├── server/                   # 服务器主程序
│   │   └── main.go              # 服务器入口文件
│   └── client/                   # 客户端工具
│       └── main.go              # 客户端入口文件
│
├── internal/                     # 内部包（不对外暴露）
│   ├── server/                  # TCP 服务器层
│   │   └── server.go           # 服务器实现、连接管理
│   │
│   ├── protocol/                # 协议层
│   │   └── parser.go           # RESP 协议解析和编码
│   │
│   ├── database/                # 数据库核心层
│   │   ├── database.go         # 数据库实例实现
│   │   └── db_manager.go       # 数据库管理器（多 DB 支持）
│   │
│   ├── datastruct/              # 数据结构层
│   │   └── data.go             # Redis 数据结构定义
│   │
│   └── command/                 # 命令层
│       ├── interface.go        # 命令接口定义
│       ├── basic.go            # 基础命令（SET/GET/DEL 等）
│       ├── list.go             # 列表命令（LPUSH/LPOP 等）
│       └── registry.go         # 命令注册表
│
├── pkg/                          # 公共包（可复用组件）
│   └── logger/                  # 日志包
│       └── logger.go           # 日志工具函数
│
├── config/                       # 配置包
│   └── config.go               # 配置结构定义
│
├── docs/                         # 文档目录
│   ├── examples.md             # 使用示例
│   └── architecture.md         # 架构说明（本文件）
│
├── data/                         # 数据目录（运行时生成）
│   ├── appendonly.aof          # AOF 持久化文件
│   └── dump.rdb                # RDB 快照文件
│
├── logs/                         # 日志目录（运行时生成）
│   └── gedis.log               # 服务器日志
│
├── bin/                          # 编译输出目录（构建时生成）
│   ├── gedis-server            # 服务器可执行文件
│   └── gedis-client            # 客户端可执行文件
│
├── go.mod                        # Go 模块定义
├── Makefile                      # 构建脚本
├── .gitignore                    # Git 忽略文件
└── README.md                     # 项目说明文档
```

## 分层架构

Gedis 采用清晰的分层架构设计，自顶向下分为：

### 1. 应用层 (cmd/)
- **server**: 服务器主程序，负责启动网络服务、加载配置
- **client**: 简单的命令行客户端工具

### 2. 网络层 (internal/server/)
- 实现 TCP 服务器
- 处理客户端连接的接受和管理
- 协调协议解析和命令执行
- 处理 SELECT 等特殊命令

### 3. 协议层 (internal/protocol/)
- 实现 Redis RESP 协议
- Request/Response 的解析和编码
- 支持的数据类型：
  - SimpleString (+)
  - Error (-)
  - Integer (:)
  - BulkString ($)
  - Array (*)

### 4. 命令层 (internal/command/)
- 实现各种 Redis 命令
- 命令注册和分发
- 参数验证和错误处理
- 可扩展的命令系统

### 5. 存储层 (internal/database/ + internal/datastruct/)
- **database**: 数据库实例和管理器
- **datastruct**: Redis 数据结构实现
  - String: 字符串
  - List: 列表
  - Hash: 哈希（待完善）
  - Set: 集合（待完善）
  - ZSet: 有序集合（待完善）

### 6. 支撑层 (pkg/ + config/)
- **logger**: 日志工具
- **config**: 配置管理

## 数据流

```
客户端请求 → TCP 连接 → RESP 解析 → 命令查找 → 命令执行 → 数据库操作 → RESP 编码 → 返回响应
```

## 核心流程

### 服务器启动流程
1. 加载配置 (config.DefaultConfig)
2. 初始化日志 (logger.Init)
3. 创建数据库管理器 (database.NewDBManager)
4. 注册默认命令 (command.InitDefaultCommands)
5. 启动 TCP 监听 (net.Listen)
6. 接受连接并处理 (handleConnection)

### 请求处理流程
1. 接受客户端连接 (listener.Accept)
2. 创建协议解析器 (protocol.NewParser)
3. 解析 RESP 请求 (parser.ParseRequest)
4. 查找对应命令 (registry.Get)
5. 执行命令 (command.Execute)
6. 编码响应 (protocol.EncodeResponse)
7. 发送回客户端 (conn.Write)

## 并发模型

- **每个连接一个 goroutine**: 每个客户端连接在独立的 goroutine 中处理
- **读写锁机制**: 每个数据库使用 sync.RWMutex 保护
  - 读操作（GET）使用读锁
  - 写操作（SET/DEL）使用写锁
- **无锁设计**: 命令执行尽量使用局部变量减少锁竞争

## 关键设计

### 1. 命令模式
使用 Command 接口统一所有命令：
```go
type Command interface {
    Execute(db *database.Database, args []string) *protocol.Response
}
```

### 2. 数据结构封装
每种数据类型独立封装，提供专门的方法：
- List: PushLeft, PushRight, PopLeft, PopRight, Range
- Hash: Set, Get, Delete, Size
- Set: Add, Remove, Contains, Members
- ZSet: Add, Remove, Score, RangeByScore

### 3. 过期策略
- DataValue 包含 ExpireTime 字段
- 每次访问时检查是否过期
- 惰性删除：访问时发现过期数据则跳过

### 4. 多数据库支持
- DBManager 管理多个 Database 实例
- 通过 SELECT 命令切换当前数据库
- 每个连接维护自己的当前数据库

## 扩展性

### 添加新命令
1. 在 `internal/command/` 创建新文件或在现有文件中添加
2. 实现 Command 接口的 Execute 方法
3. 在 `registry.go` 的 InitDefaultCommands() 中注册

### 添加新数据类型
1. 在 `internal/datastruct/data.go` 中定义新结构
2. 实现必要的方法
3. 在命令中使用新类型

### 添加持久化
1. 在 `internal/database/` 添加 RDB/AOF 模块
2. 在 server.Start() 中加载持久化数据
3. 定期或在关闭时保存数据

## 性能考虑

1. **内存池**: 可使用 sync.Pool 复用 buffer
2. **批量操作**: 支持 MSET/MGET 等批量命令
3. **管道**: 支持 pipeline 减少网络往返
4. **过期清理**: 定期后台清理过期键
5. **索引优化**: 为 KEYS 等操作添加索引
