package qumsg

import (
	"context"
	"iter"
	"maps"
)

// Envelope wraps a message with the metadata that travels with it through a
// transport, such as a request ID or the user that caused it. Metadata are
// strings so that any transport can store them.
//
// An Envelope is immutable: With returns a new envelope and leaves the
// original unchanged, so it can be shared between goroutines.
type Envelope struct {
	message  any
	metadata map[string]string
}

// NewEnvelope returns an envelope for message without metadata.
func NewEnvelope(message any) Envelope {
	return Envelope{message: message}
}

// Message returns the wrapped message.
func (e Envelope) Message() any { return e.message }

// With returns a copy of the envelope with the metadata key set to value.
func (e Envelope) With(key, value string) Envelope {
	metadata := make(map[string]string, len(e.metadata)+1)
	maps.Copy(metadata, e.metadata)
	metadata[key] = value
	return Envelope{message: e.message, metadata: metadata}
}

// Get returns the metadata value for key and whether it is set.
func (e Envelope) Get(key string) (string, bool) {
	value, ok := e.metadata[key]
	return value, ok
}

// Metadata iterates over the metadata in no particular order. Transports use
// it to store them next to the message.
func (e Envelope) Metadata() iter.Seq2[string, string] {
	return maps.All(e.metadata)
}

// Propagator carries values of the context across a transport: Inject copies
// them from the context of the sender into the envelope, and Extract puts them
// back into the context of the worker that dispatches the message.
type Propagator interface {
	Inject(ctx context.Context, e Envelope) Envelope
	Extract(ctx context.Context, e Envelope) context.Context
}

// ContextValue returns a Propagator for a string stored in the context under
// key, which travels as the metadata entry name. Inject skips it when the
// context has no string under key.
//
//	type requestIDKey struct{}
//
//	qumsg.WithPropagators(qumsg.ContextValue("request_id", requestIDKey{}))
func ContextValue(name string, key any) Propagator {
	return contextValue{name: name, key: key}
}

type contextValue struct {
	name string
	key  any
}

func (p contextValue) Inject(ctx context.Context, e Envelope) Envelope {
	if value, ok := ctx.Value(p.key).(string); ok {
		return e.With(p.name, value)
	}
	return e
}

func (p contextValue) Extract(ctx context.Context, e Envelope) context.Context {
	if value, ok := e.Get(p.name); ok {
		return context.WithValue(ctx, p.key, value)
	}
	return ctx
}

// WithPropagators adds propagators to the bus. The bus runs Inject on every
// message it sends to a transport (see Bus.Route), and Consume runs Extract
// on every message it receives, both in the order they were added. It panics if a propagator
// is nil.
func WithPropagators(propagators ...Propagator) Option {
	for _, p := range propagators {
		if p == nil {
			panic("qumsg: nil propagator")
		}
	}
	return func(b *Bus) { b.propagators = append(b.propagators, propagators...) }
}

// envelope wraps message and lets the propagators of the bus fill it from ctx.
func (b *Bus) envelope(ctx context.Context, message any) Envelope {
	e := NewEnvelope(message)
	for _, p := range b.propagators {
		e = p.Inject(ctx, e)
	}
	return e
}

// extract restores the metadata of e into ctx with the propagators of the bus.
func (b *Bus) extract(ctx context.Context, e Envelope) context.Context {
	for _, p := range b.propagators {
		ctx = p.Extract(ctx, e)
	}
	return ctx
}
