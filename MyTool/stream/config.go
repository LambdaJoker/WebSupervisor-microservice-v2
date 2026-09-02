package stream

import (
	"encoding/json"
	"flag"
	"os"
	"regexp"
)

// Config 通信库配置
type Config struct {
	Redis  RedisConfig            `json:"redis"`
	Stream StreamConfig           `json:"stream"`
	Custom map[string]interface{} `json:"custom"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type StreamConfig struct {
	ConsumerStream  string `json:"consumer_stream"`
	ConsumerGroup   string `json:"consumer_group"`
	GoroutineNum    int    `json:"goroutine_num"`
	GetTimeoutMs    int    `json:"get_timeout_ms"`
	MaxMessageBytes int    `json:"max_message_bytes"`
	CacheKeyPrefix  string `json:"cache_key_prefix"`
}

var (
	cfg            Config
	StreamName     string
	ServiceName    string
	ConsumerGroup  string
	MaxMsgSize     int
	Custom         map[string]interface{}
	CacheKeyPrefix string
)

func ParseOsArgs() *string {
	return flag.String("config_path", "./config.json", "配置文件路径")
}

// LoadConfig 从 JSON 文件读取配置，并解析 ${} 环境变量。
func LoadConfig() error {
	path := ParseOsArgs()
	flag.Parse()

	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}

	expanded := expandEnvVars(string(data))
	if err := json.Unmarshal([]byte(expanded), &cfg); err != nil {
		return err
	}

	StreamName = cfg.Stream.ConsumerStream
	ServiceName = cfg.Stream.ConsumerStream
	ConsumerGroup = cfg.Stream.ConsumerGroup
	MaxMsgSize = cfg.Stream.MaxMessageBytes
	if MaxMsgSize <= 0 {
		MaxMsgSize = 1024 * 1024
	}
	Custom = cfg.Custom
	CacheKeyPrefix = cfg.Stream.CacheKeyPrefix
	if CacheKeyPrefix == "" {
		CacheKeyPrefix = "stream:"
	}
	return nil
}

// expandEnvVars 替换字符串中的 ${VAR} 为环境变量值。
func expandEnvVars(s string) string {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	return re.ReplaceAllStringFunc(s, func(match string) string {
		varName := match[2 : len(match)-1]
		return os.Getenv(varName)
	})
}
