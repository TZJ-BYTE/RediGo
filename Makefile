.PHONY: build run clean test help server client

# Go 参数
GO = go
BINARY_SERVER = bin/gedis-server
BINARY_CLIENT = bin/gedis-client

# 默认目标
all: build

# 构建服务器
build: build-server build-client

build-server:
	@echo "构建服务器..."
	@mkdir -p bin
	$(GO) build -o $(BINARY_SERVER) cmd/server/main.go

build-client:
	@echo "构建客户端..."
	@mkdir -p bin
	$(GO) build -o $(BINARY_CLIENT) cmd/client/main.go

# 运行服务器
run: build-server
	@echo "启动 Gedis 服务器..."
	./$(BINARY_SERVER)

# 运行客户端
client: build-client
	@echo "启动 Gedis 客户端..."
	./$(BINARY_CLIENT)

# 清理构建文件
clean:
	@echo "清理构建文件..."
	rm -rf bin/
	rm -rf logs/
	rm -rf data/

# 运行测试
test:
	@echo "运行测试..."
	$(GO) test -v ./...

# 下载依赖
deps:
	@echo "下载依赖..."
	$(GO) mod tidy

# 格式化代码
fmt:
	@echo "格式化代码..."
	$(GO) fmt ./...

# 帮助信息
help:
	@echo "Gedis Makefile"
	@echo ""
	@echo "可用命令:"
	@echo "  make build     - 构建服务器和客户端"
	@echo "  make run       - 运行服务器"
	@echo "  make client    - 运行客户端"
	@echo "  make clean     - 清理构建文件"
	@echo "  make test      - 运行测试"
	@echo "  make deps      - 下载依赖"
	@echo "  make fmt       - 格式化代码"
