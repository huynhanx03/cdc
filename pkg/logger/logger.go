package logger

import (
	"log/slog"
	"os"
)

// Init sets up the global slog logger.
// mode: "json" for production, anything else for human-readable text.
func Init(mode string) {
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}

	var handler slog.Handler
	if mode == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))
}
