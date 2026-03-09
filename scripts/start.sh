#!/bin/bash

# Gedis 快速启动脚本

echo "======================================"
echo "  Gedis - Go Redis Implementation"
echo "======================================"
echo ""

# 检查 Go 是否安装
if ! command -v go &> /dev/null; then
    echo "错误：未找到 Go，请先安装 Go 1.21+"
    exit 1
fi

echo "Go 版本:"
go version
echo ""

# 创建必要目录
mkdir -p logs data bin

# 下载依赖
echo "正在下载依赖..."
go mod tidy
echo ""

# 构建项目
echo "正在构建项目..."
make build
echo ""

echo "======================================"
echo "  构建完成!"
echo "======================================"
echo ""
echo "可执行文件位置:"
echo "  服务器：./bin/gedis-server"
echo "  客户端：./bin/gedis-client"
echo ""
echo "启动方式:"
echo "  1. 直接运行服务器: make run"
echo "  2. 后台运行服务器: nohup ./bin/gedis-server > logs/server.log 2>&1 &"
echo "  3. 使用客户端连接: make client"
echo "  4. 使用 redis-cli: redis-cli -h 127.0.0.1 -p 6379"
echo ""
echo "日志文件：./logs/gedis.log"
echo "数据目录：./data/"
echo ""
