// Package qumsg implements a synchronous message bus inspired by Symfony
// Messenger: messages are plain Go values dispatched to typed handlers, and
// the whole dispatch can be wrapped with middleware.
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
	// the concrete type of the message.
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

// Bus dispatches messages to the handlers registered for their exact type.
// Register all handlers before dispatching; once configured, a Bus is safe for
// concurrent use.
type Bus struct {
	dispatch HandleFunc

	mu     sync.RWMutex
	routes map[reflect.Type]route
	names  map[string]reflect.Type
}

// route runs the handlers registered for one message type.
type route interface {
	handle(ctx context.Context, message any) error
}

// handlers keeps the handlers of type T typed, so a dispatch asserts the
// message once instead of once per handler.
type handlers[T any] []func(context.Context, T) error

func (hs handlers[T]) handle(ctx context.Context, message any) error {
	msg := message.(T)
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

// New creates a bus. The first middleware is the outermost one.
func New(middlewares ...Middleware) *Bus {
	b := &Bus{
		routes: make(map[reflect.Type]route),
		names:  make(map[string]reflect.Type),
	}

	next := b.handle
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] == nil {
			panic(fmt.Sprintf("qumsg: nil middleware at position %d", i))
		}
		next = middlewares[i](next)
	}
	b.dispatch = next

	return b
}

// Register adds a handler for messages of the exact type T. Handlers for the
// same type run in registration order.
func (b *Bus) Register[T any](handler func(context.Context, T) error) error {
	if handler == nil {
		return ErrNilHandler
	}

	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return fmt.Errorf("%w: %s", ErrInterfaceType, typ)
	}

	name, named, err := messageName[T]()
	if err != nil {
		return err
	}
	if named && name == "" {
		return fmt.Errorf("%w: %s", ErrEmptyName, typ)
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

// Dispatch runs the middleware chain and the handlers registered for the
// concrete type of message. Handlers run sequentially and the first error
// stops the execution. The context is checked before each handler.
func (b *Bus) Dispatch(ctx context.Context, message any) error {
	return b.dispatch(ctx, message)
}

func (b *Bus) handle(ctx context.Context, message any) error {
	if message == nil {
		return ErrNilMessage
	}

	b.mu.RLock()
	r, ok := b.routes[reflect.TypeOf(message)]
	b.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %T", ErrNoHandler, message)
	}

	return r.handle(ctx, message)
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
