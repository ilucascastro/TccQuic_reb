package test_client

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultPipeline      = false
	defaultFOVTracePath  = "data/user_fov.csv"
	defaultFOVTraceFPS   = 30
	defaultSegmentDurationSeconds = 1
)

// EnvironmentConfig centralizes values that previously lived as scattered
// constants or inline env lookups. StartTestClient resolves it once and
// passes the struct around so other components stay deterministic and testable.
type EnvironmentConfig struct {
	Pipeline       bool
	FOVTracePath   string
	FOVTraceFPS    int
	StatisticsPath string
	SummaryPath    string
	FOVDeliveryPath string
	FOVGoodputPath  string
}

func ResolveEnvironmentConfig() EnvironmentConfig {
	cfg := EnvironmentConfig{
		Pipeline:       defaultPipeline,
		FOVTracePath:   getEnvOrDefault("FOV_TRACE_PATH", defaultFOVTracePath),
		FOVTraceFPS:    getEnvInt("FOV_TRACE_FPS", defaultFOVTraceFPS),
	}

	pid := os.Getpid()
	cfg.StatisticsPath = fmt.Sprintf("statistics-%d.csv", pid)
	cfg.SummaryPath = fmt.Sprintf("statistics-summary-%d.csv", pid)
	cfg.FOVDeliveryPath = fmt.Sprintf("fov-delivery-%d.csv", pid)
	cfg.FOVGoodputPath = fmt.Sprintf("fov-goodput-%d.csv", pid)

	return cfg
}

func getEnvOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
