package config

// Config 服务器配置
type Config struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
	
	// 数据库配置
	DBCount int `yaml:"db_count" json:"db_count"`
	
	// 内存配置
	MaxMemory int64 `yaml:"max_memory" json:"max_memory"`
	
	// 持久化配置
	AOFEnabled bool   `yaml:"aof_enabled" json:"aof_enabled"`
	AOFPath    string `yaml:"aof_path" json:"aof_path"`
	RDBPath    string `yaml:"rdb_path" json:"rdb_path"`
	
	// 日志配置
	LogLevel string `yaml:"log_level" json:"log_level"`
	LogPath  string `yaml:"log_path" json:"log_path"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Host:      "0.0.0.0",
		Port:      16379,  // 修改为 16379，避免与 Redis 的 6379 冲突
		DBCount:   16,
		MaxMemory: 256 * 1024 * 1024, // 256MB
		AOFEnabled: true,
		AOFPath:   "./data/appendonly.aof",
		RDBPath:   "./data/dump.rdb",
		LogLevel:  "info",
		LogPath:   "./logs/gedis.log",
	}
}
