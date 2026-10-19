package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	return &Config{
		ShutdownTimeout: 30,
		Metrics:         MetricsConfig{Addr: "127.0.0.1:9090"},
		Tracing: TracingConfig{
			Addr: "collector.example.com:4318", MaxBatchSize: 512,
			BatchTimeout: 5, ExportTimeout: 10, MaxQueueSize: 2048,
			SamplingRatio: 0.1,
		},
	}
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, validConfig().Validate())
	config := validConfig()
	config.Tracing.SamplingRatio = 2
	assert.Error(t, config.Validate())
}

func TestNewUnitUsesProductionSafeDefaults(t *testing.T) {
	u := NewUnit(nil, "test", "v1", "test")
	require.NoError(t, u.config.Validate())
	assert.Equal(t, "127.0.0.1:9090", u.config.Metrics.Addr)
	assert.False(t, u.config.Tracing.Insecure)
}

func TestRedactConfiguration(t *testing.T) {
	redacted, err := redactConfiguration(map[string]any{
		"database": map[string]any{"password": "secret", "host": "db"},
	})
	require.NoError(t, err)
	database := redacted.(map[string]any)["database"].(map[string]any)
	assert.Equal(t, "[REDACTED]", database["password"])
	assert.Equal(t, "db", database["host"])
}
