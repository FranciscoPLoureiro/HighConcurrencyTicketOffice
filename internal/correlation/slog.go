package correlation

import (
	"context"
	"log/slog"
)

// LogKey is the field name every correlated line carries.
const LogKey = "correlation_id"

// Handler adds the correlation id to every record that has one in context.
//
// The alternative is passing a logger down through every function that might
// log, or remembering to add the field at each call site — and "remembering" is
// the part that fails. The lines that get forgotten are the ones written in a
// hurry on a failure path, which are exactly the lines somebody will be trying
// to correlate at three in the morning.
//
// A handler instead of a logger means it works for anything already holding a
// context, including code that has no idea this exists.
type Handler struct {
	slog.Handler
}

// NewHandler wraps an existing handler.
func NewHandler(inner slog.Handler) *Handler { return &Handler{Handler: inner} }

// Handle adds the id, when there is one.
//
// Records outside a request — startup, shutdown, a background sweep — pass
// through untouched. Inventing an id for them would produce a field that looks
// joinable and joins to nothing, which is worse than an absent field because it
// costs somebody a search to discover.
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	if id := From(ctx); id != "" {
		record.AddAttrs(slog.String(LogKey, id))
	}
	return h.Handler.Handle(ctx, record)
}

// WithAttrs and WithGroup have to rewrap, or the derived logger loses the
// correlation behaviour and does so silently — the embedded handler's own
// methods would return the inner type.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &Handler{Handler: h.Handler.WithGroup(name)}
}
