// Package qumsg implements a synchronous message bus inspired by Symfony
// Messenger: messages are plain Go values dispatched to typed handlers, and
// the whole dispatch can be wrapped with middleware.
package qumsg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

	mu       sync.RWMutex
	handlers map[reflect.Type][]HandleFunc
	names    map[string]reflect.Type
}

// New creates a bus. The first middleware is the outermost one.
func New(middlewares ...Middleware) *Bus {
	b := &Bus{
		handlers: make(map[reflect.Type][]HandleFunc),
		names:    make(map[string]reflect.Type),
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
func Register[T any](bus *Bus, handler func(context.Context, T) error) error {
	if handler == nil {
		return ErrNilHandler
	}

	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return fmt.Errorf("%w: %s", ErrInterfaceType, typ)
	}

	name, named := messageName[T](typ)
	if named && name == "" {
		return fmt.Errorf("%w: %s", ErrEmptyName, typ)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()

	if named {
		if other, ok := bus.names[name]; ok && other != typ {
			return fmt.Errorf("%w: %q is used by %s, cannot register %s", ErrDuplicateName, name, other, typ)
		}
		bus.names[name] = typ
	}

	bus.handlers[typ] = append(bus.handlers[typ], func(ctx context.Context, message any) error {
		return handler(ctx, message.(T))
	})

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
	handlers := b.handlers[reflect.TypeOf(message)]
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return fmt.Errorf("%w: %T", ErrNoHandler, message)
	}

	for _, h := range handlers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h(ctx, message); err != nil {
			return err
		}
	}

	return nil
}

// messageName returns the alias of T if it implements NamedMessage. For
// pointer types it calls Name on a pointer to a zero value instead of a nil
// pointer, so value-receiver methods do not panic.
func messageName[T any](typ reflect.Type) (string, bool) {
	var sample T
	if typ.Kind() == reflect.Pointer {
		sample = reflect.New(typ.Elem()).Interface().(T)
	}

	named, ok := any(sample).(NamedMessage)
	if !ok {
		return "", false
	}

	return named.Name(), true
}
