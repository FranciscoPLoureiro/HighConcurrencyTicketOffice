package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadFallsBackToDefaultsWhenNothingIsSet(t *testing.T) {
	clearEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned an error with no environment set: %v", err)
	}

	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, defaultShutdownTimeout)
	}
	if cfg.LogLevel != defaultLogLevel {
		t.Errorf("LogLevel = %s, want %s", cfg.LogLevel, defaultLogLevel)
	}
	if cfg.PostgresDSN == "" || cfg.RedisAddr == "" {
		t.Error("defaults must leave the service addressable, got empty DSN or Redis address")
	}
}

func TestLoadReadsEveryValueFromTheEnvironment(t *testing.T) {
	clearEnv(t)
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("POSTGRES_DSN", "postgres://elsewhere/db")
	t.Setenv("REDIS_ADDR", "redis.internal:6379")
	t.Setenv("REDIS_PASSWORD", "hunter2")
	t.Setenv("SHUTDOWN_TIMEOUT", "45s")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("CAMPAIGN_ID", "noites-do-parque")
	t.Setenv("TOTAL_TICKETS", "250")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want no error", err)
	}

	want := Config{
		HTTPAddr:        ":9999",
		PostgresDSN:     "postgres://elsewhere/db",
		RedisAddr:       "redis.internal:6379",
		RedisPassword:   "hunter2",
		ShutdownTimeout: 45 * time.Second,
		LogLevel:        slog.LevelDebug,
		CampaignID:      "noites-do-parque",
		TotalTickets:    250,
	}
	if cfg != want {
		t.Errorf("Load() = %+v, want %+v", cfg, want)
	}
}

// Orchestrators routinely inject an empty string for a variable that was never
// given a value. Honouring that literally would leave the server with no listen
// address, which is never what the operator meant.
func TestBlankValuesAreTreatedAsUnset(t *testing.T) {
	clearEnv(t)
	t.Setenv("HTTP_ADDR", "   ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want no error", err)
	}
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want the default %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
}

func TestSurroundingWhitespaceIsTrimmed(t *testing.T) {
	clearEnv(t)
	t.Setenv("REDIS_ADDR", "  redis.internal:6379\t")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want no error", err)
	}
	if cfg.RedisAddr != "redis.internal:6379" {
		t.Errorf("RedisAddr = %q, want the trimmed value", cfg.RedisAddr)
	}
}

func TestUnparseableValuesAreRejectedAndNameTheirVariable(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "duration", key: "SHUTDOWN_TIMEOUT", value: "quite a while"},
		{name: "log level", key: "LOG_LEVEL", value: "chatty"},
		{name: "integer", key: "TOTAL_TICKETS", value: "one hundred"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with %s=%q returned no error", tc.key, tc.value)
			}
			// The variable name is the whole value of the message: an
			// operator reading a crash log needs to know what to fix.
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("error %q does not name the offending variable %s", err, tc.key)
			}
		})
	}
}

// An operator repairing a deployment should see every problem in one pass
// rather than discovering them one restart at a time.
func TestEveryProblemIsReportedInOnePass(t *testing.T) {
	clearEnv(t)
	t.Setenv("SHUTDOWN_TIMEOUT", "not a duration")
	t.Setenv("LOG_LEVEL", "not a level")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() returned no error for two invalid values")
	}

	for _, key := range []string{"SHUTDOWN_TIMEOUT", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q omits %s; errors must accumulate, not short-circuit", err, key)
		}
	}
}

// A campaign with no tickets is not a campaign. Letting zero through would
// produce a service that starts happily and refuses every purchase as sold out.
func TestNonPositiveTicketCountIsRejected(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		t.Run(value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("TOTAL_TICKETS", value)

			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted TOTAL_TICKETS=%s", value)
			}
		})
	}
}

func TestNonPositiveShutdownTimeoutIsRejected(t *testing.T) {
	clearEnv(t)
	// Zero would make graceful shutdown a no-op, quietly turning every
	// deploy into a hard kill of in-flight work.
	t.Setenv("SHUTDOWN_TIMEOUT", "0s")

	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a zero shutdown timeout")
	}
}

// clearEnv isolates a test from whatever the developer happens to have exported.
// t.Setenv restores the previous value when the test ends.
func clearEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"HTTP_ADDR",
		"POSTGRES_DSN",
		"REDIS_ADDR",
		"REDIS_PASSWORD",
		"SHUTDOWN_TIMEOUT",
		"LOG_LEVEL",
		"CAMPAIGN_ID",
		"TOTAL_TICKETS",
	} {
		t.Setenv(key, "")
	}
}
