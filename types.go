package qumsg

import (
	"context"
	"errors"
	"reflect"
)

// ErrZeroType is returned when a Type was not created with TypeOf.
var ErrZeroType = errors.New("qumsg: zero Type, create it with TypeOf")

// Type is a message type as a value. A generic method cannot be part of an
// interface or be called on a list of types, so code that handles a family
// of messages (the events of a package, say) would need one Register[T] or
// Route[T] call per type, repeated wherever the family is used. TypeOf
// captures T once, and the list can then be passed to RegisterFor,
// RouteTypes or a transport:
//
//	var Events = []qumsg.Type{qumsg.TypeOf[UserCreated](), qumsg.TypeOf[UserDeleted]()}
//
//	bus.RouteTypes(transport, Events...)
//
// The zero Type is invalid.
type Type struct {
	typ   reflect.Type
	name  string
	named bool
	err   error

	register func(b *Bus, handler HandleFunc) error
	decode   func(unmarshal func(target any) error) (any, error)
}

// TypeOf returns the Type of T. It never fails: if T is not a valid message
// type (see Bus.Register), Err reports why, and so does any registration.
func TypeOf[T any]() Type {
	name, named, err := checkType[T]()
	return Type{
		typ:   reflect.TypeFor[T](),
		name:  name,
		named: named,
		err:   err,
		register: func(b *Bus, handler HandleFunc) error {
			return b.Register(func(ctx context.Context, msg T) error { return handler(ctx, msg) })
		},
		decode: func(unmarshal func(target any) error) (any, error) {
			var msg T
			if err := unmarshal(&msg); err != nil {
				return nil, err
			}
			return msg, nil
		},
	}
}

// Reflect returns the reflect.Type of the message type, or nil for the zero
// Type.
func (t Type) Reflect() reflect.Type { return t.typ }

// String returns the Go name of the type.
func (t Type) String() string {
	if t.typ == nil {
		return "<zero qumsg.Type>"
	}
	return t.typ.String()
}

// Name returns the alias of the type and whether it implements NamedMessage.
func (t Type) Name() (string, bool) { return t.name, t.named }

// Err reports whether the type can be registered: it returns ErrZeroType,
// ErrInterfaceType, ErrEmptyName, ErrNamePanic or nil.
func (t Type) Err() error {
	if t.register == nil {
		return ErrZeroType
	}
	return t.err
}

// Decode creates a zero message of the type, passes a pointer to it to
// unmarshal and returns the message, not the pointer. Transports use it to
// decode payloads without being generic:
//
//	msg, err := typ.Decode(func(target any) error { return json.Unmarshal(payload, target) })
func (t Type) Decode(unmarshal func(target any) error) (any, error) {
	if t.decode == nil {
		return nil, ErrZeroType
	}
	return t.decode(unmarshal)
}

// RegisterFor registers the same handler for every type in types, as if
// Register were called for each one with a handler that receives the message
// as any. Use it for cross-cutting handlers such as an audit log.
//
// All types are validated before registering any; a later failure, such as
// a duplicate name, leaves the previous types registered.
func (b *Bus) RegisterFor(handler HandleFunc, types ...Type) error {
	if handler == nil {
		return ErrNilHandler
	}
	for _, t := range types {
		if err := t.Err(); err != nil {
			return err
		}
	}
	for _, t := range types {
		if err := t.register(b, handler); err != nil {
			return err
		}
	}
	return nil
}

// RouteTypes is Route for every type in types, with the same rules as
// RegisterFor.
func (b *Bus) RouteTypes(t Transport, types ...Type) error {
	if t == nil {
		return ErrNilTransport
	}
	return b.RegisterFor(t.Send, types...)
}
