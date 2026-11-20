package config

import (
	"log/slog"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	for _, test := range []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"unknown", slog.LevelInfo},
	} {
		got, err := parseLogLevel(test.input)
		if test.input == "unknown" {
			if err == nil {
				t.Fatalf("%q should be rejected by parser", test.input)
			}
			got = slog.LevelInfo
		} else if err != nil {
			t.Fatalf("parse %q: %v", test.input, err)
		}
		if got != test.want {
			t.Errorf("parse %q = %s, want %s", test.input, got, test.want)
		}
	}
}
