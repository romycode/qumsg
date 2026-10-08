// Package qumsg implements a synchronous message bus inspired by Symfony
// Messenger: messages are plain Go values dispatched to typed handlers, and
// the whole dispatch can be wrapped with middleware.
//
// The bus does not know whether a message is a command or an event: its
// options decide whether a message without handlers is an error and whether
// the first failing handler stops the rest. An application that needs both
// behaviours creates one bus for each.
package qumsg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
)

var (
	// ErrNoHandler is returned by Dispatch when no handler is registered for
	// the concrete type of the message, unless the bus was created with
	// AllowNoHandler.
	ErrNoHandler = errors.New("qumsg: no handler registered for message type")
	// ErrNilMessage is returned by Dispatch when the message is nil.
	ErrNilMessage = errors.New("qumsg: nil message")
	// ErrNilHandler is returned by Register when the handler is nil.
	ErrNilHandler = errors.New("qumsg: nil handler")
	// ErrInterfaceType is returned by Register when T is an interface type.
	// Dispatch resolves by the concrete type of the message, so a handler for
	// an interface type could never be called.
	ErrInterfaceType = errors.New("qumsg: message type must be concrete, not an interface")
	// ErrEmptyName is returned by Register when the message type implements
	// NamedMessage and Name returns an empty string.
	ErrEmptyName = errors.New("qumsg: message name is empty")
	// ErrNamePanic is returned by Register when Name panics on the zero value
	// of the message type, usually because it depends on the message fields.
	ErrNamePanic = errors.New("qumsg: message Name panicked")
	// ErrDuplicateName is returned by Register when another message type is
	// already registered with the same name.
	ErrDuplicateName = errors.New("qumsg: message name already registered for another type")
)

// NamedMessage lets a message type provide an optional alias. Name must be
// constant for the type and must not depend on the message contents.
type NamedMessage interface {
	Name() string
}

// HandleFunc processes a message of any type. It is the unit wrapped by
// middleware.
type HandleFunc func(ctx context.Context, message any) error

// Middleware wraps the complete dispatch of a message.
type Middleware func(next HandleFunc) HandleFunc

// Option configures a Bus.
type Option func(*Bus)

// WithMiddleware adds middleware to the bus. The first one is the outermost,
// and several WithMiddleware options add their middleware in order. It
// panics if a middleware is nil.
func WithMiddleware(middlewares ...Middleware) Option {
	for i, mw := range middlewares {
		if mw == nil {
			panic(fmt.Sprintf("qumsg: nil middleware at position %d", i))
		}
	}
	return func(b *Bus) { b.middlewares = append(b.middlewares, middlewares...) }
}

// AllowNoHandler makes Dispatch return nil instead of ErrNoHandler for a
// message without handlers, as fits notifications that may have no
// subscribers.
func AllowNoHandler() Option {
	return func(b *Bus) { b.allowNoHandler = true }
}

// ContinueOnError makes Dispatch run every handler even if some fail, and
// return their errors joined with errors.Join; a single error is returned
// unwrapped. By default the first error stops the remaining handlers.
func ContinueOnError() Option {
	return func(b *Bus) { b.continueOnError = true }
}

// Bus dispatches messages to the handlers registered for their exact type.
// Register all handlers before dispatching; once configured, a Bus is safe for
// concurrent use.
type Bus struct {
	dispatch        HandleFunc // middleware + routing + handlers
	consume         HandleFunc // middleware + handlers, for Consume
	router          *Router
	middlewares     []Middleware // only used by New
	propagators     []Propagator
	allowNoHandler  bool
	continueOnError bool

	mu     sync.RWMutex
	routes map[reflect.Type]route
	names  map[string]reflect.Type
}

// route runs the handlers registered for one message type.
type route interface {
	// handle runs the handlers until the first error or, with all, every
	// handler, joining their errors.
	handle(ctx context.Context, message any, all bool) error
}

// handlers keeps the handlers of type T typed, so a dispatch asserts the
// message once instead of once per handler.
type handlers[T any] []func(context.Context, T) error

func (hs handlers[T]) handle(ctx context.Context, message any, all bool) error {
	msg := message.(T)
	if all {
		return hs.handleAll(ctx, msg)
	}
	for _, h := range hs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (hs handlers[T]) handleAll(ctx context.Context, msg T) error {
	var errs []error
	for _, h := range hs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := h(ctx, msg); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 1 {
		return errs[0] // unwrapped, as without ContinueOnError
	}
	return errors.Join(errs...)
}

// New creates a bus. By default a message without handlers is an error
// (ErrNoHandler) and the first failing handler stops the rest; see
// AllowNoHandler and ContinueOnError. It panics if an option is nil.
func New(opts ...Option) *Bus {
	b := &Bus{
		routes: make(map[reflect.Type]route),
		names:  make(map[string]reflect.Type),
	}

	for i, opt := range opts {
		if opt == nil {
			panic(fmt.Sprintf("qumsg: nil option at position %d", i))
		}
		opt(b)
	}

	b.dispatch = b.chain(b.sendOrHandle)
	b.consume = b.chain(b.handle)
	b.middlewares = nil

	return b
}

// Register adds a handler for messages of the exact type T. Handlers for the
// same type run in registration order.
func (b *Bus) Register[T any](handler func(context.Context, T) error) error {
	if handler == nil {
		return ErrNilHandler
	}

	typ := reflect.TypeFor[T]()
	name, named, err := checkType[T]()
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if named {
		if other, ok := b.names[name]; ok && other != typ {
			return fmt.Errorf("%w: %q is used by %s, cannot register %s", ErrDuplicateName, name, other, typ)
		}
		b.names[name] = typ
	}

	// Copy on write: a dispatch that already read the route keeps its slice.
	current, _ := b.routes[typ].(handlers[T])
	b.routes[typ] = append(slices.Clip(current), handler)

	return nil
}

// chain wraps next with the middleware of the bus, the first one outermost.
func (b *Bus) chain(next HandleFunc) HandleFunc {
	for i := len(b.middlewares) - 1; i >= 0; i-- {
		next = b.middlewares[i](next)
	}
	return next
}

// Dispatch runs the middleware chain and the handlers registered for the
// concrete type of message. Handlers run sequentially; the first error stops
// the execution unless the bus was created with ContinueOnError. The context
// is checked before each handler: once it is done, the remaining handlers
// are skipped.
//
// If the router of the bus routes the type of message, Dispatch then sends
// it to its transport, as if the route were the last handler: a handler
// error prevents the send unless the bus was created with ContinueOnError,
// and a routed type needs no local handlers.
func (b *Bus) Dispatch(ctx context.Context, message any) error {
	return b.dispatch(ctx, message)
}

// sendOrHandle handles message and, if the router routes its type, sends it
// to its transport.
func (b *Bus) sendOrHandle(ctx context.Context, message any) error {
	if b.router == nil || message == nil {
		return b.handle(ctx, message)
	}
	t, routed := b.router.transport(reflect.TypeOf(message))
	if !routed {
		return b.handle(ctx, message)
	}

	var err error
	if r, ok := b.lookup(message); ok {
		err = r.handle(ctx, message, b.continueOnError)
		if err != nil && !b.continueOnError {
			return err
		}
	}
	sendErr := t.Send(ctx, b.envelope(ctx, message))
	switch {
	case err == nil:
		return sendErr // unwrapped, like a handler error
	case sendErr == nil:
		return err
	default:
		return errors.Join(err, sendErr)
	}
}

func (b *Bus) handle(ctx context.Context, message any) error {
	if message == nil {
		return ErrNilMessage
	}
	r, ok := b.lookup(message)
	if !ok {
		if b.allowNoHandler {
			return nil
		}
		return fmt.Errorf("%w: %T", ErrNoHandler, message)
	}
	return r.handle(ctx, message, b.continueOnError)
}

// lookup returns the handlers of the concrete type of message.
func (b *Bus) lookup(message any) (route, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	r, ok := b.routes[reflect.TypeOf(message)]
	return r, ok
}

// checkType validates T as a message type: it must be concrete, and its
// alias, if any, must be non-empty and must not panic.
func checkType[T any]() (name string, named bool, err error) {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return "", false, fmt.Errorf("%w: %s", ErrInterfaceType, typ)
	}
	name, named, err = messageName[T]()
	if err != nil {
		return "", false, err
	}
	if named && name == "" {
		return "", false, fmt.Errorf("%w: %s", ErrEmptyName, typ)
	}
	return name, named, nil
}

// messageName returns the alias of T if it implements NamedMessage. For
// pointer types it calls Name on a pointer to a zero value instead of a nil
// pointer, so value-receiver methods do not panic. A panic inside Name is
// returned as ErrNamePanic.
func messageName[T any]() (name string, named bool, err error) {
	typ := reflect.TypeFor[T]()

	var sample T
	if typ.Kind() == reflect.Pointer {
		sample = reflect.New(typ.Elem()).Interface().(T)
	}

	m, ok := any(sample).(NamedMessage)
	if !ok {
		return "", false, nil
	}

	defer func() {
		if r := recover(); r != nil {
			name, named, err = "", true, fmt.Errorf("%w: %s: %v", ErrNamePanic, typ, r)
		}
	}()

	return m.Name(), true, nil
}
