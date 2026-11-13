package qumsg

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

var (
	// ErrAlreadyRouted is returned by Bus.Route and Bus.RouteTypes when the
	// message type is already routed to another transport.
	ErrAlreadyRouted = errors.New("qumsg: message type already routed to another transport")
	// ErrNoRoutes is returned by Consume when the transport is a Decoder and
	// the bus routes no message type to it, so it could not decode anything.
	ErrNoRoutes = errors.New("qumsg: no message types routed to the transport")
)

// Route makes the bus process messages of the exact type T asynchronously
// through t: Dispatch sends them to t instead of running their handlers, and
// Consume runs the handlers when it receives them from t. Each type goes to at
// most one transport. T follows the rules of Register: it must be concrete
// and its alias, if any, must be valid and unique within the bus.
//
//	bus.Register(sendWelcomeEmail)
//	bus.Route[SendWelcomeEmail](transport)
//
// The producer and the worker configure the same routes, for example by
// building their buses with the same function; the producer does not need
// the handlers. Configure the routes before dispatching.
func (b *Bus) Route[T any](t Transport) error {
	return b.RouteTypes(t, TypeOf[T]())
}

// RouteTypes is Route for every type in types. It routes all of them or, if
// any is invalid, already routed to another transport or uses a taken alias,
// none.
func (b *Bus) RouteTypes(t Transport, types ...Type) error {
	if t == nil {
		return ErrNilTransport
	}
	// Transports are compared with == here and in typesFor; comparing two
	// interfaces holding the same uncomparable type panics. Once only
	// comparable transports are stored, comparing any t against them is safe.
	if typ := reflect.TypeOf(t); !typ.Comparable() {
		return fmt.Errorf("%w: %s", ErrUncomparableTransport, typ)
	}
	for _, typ := range types {
		if err := typ.Err(); err != nil {
			return err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	pending := make(map[string]reflect.Type)
	for _, typ := range types {
		if other, ok := b.transports[typ.typ]; ok && other != t {
			return fmt.Errorf("%w: %s", ErrAlreadyRouted, typ)
		}
		name, named := typ.Name()
		if !named {
			continue
		}
		owner, ok := b.names[name]
		if !ok {
			owner, ok = pending[name]
		}
		if ok && owner != typ.typ {
			return fmt.Errorf("%w: %q is used by %s, cannot route %s", ErrDuplicateName, name, owner, typ)
		}
		pending[name] = typ.typ
	}
	for _, typ := range types {
		b.transports[typ.typ] = t
		b.types[typ.typ] = typ
		if name, named := typ.Name(); named {
			b.names[name] = typ.typ
		}
	}
	return nil
}

// transport returns the transport of the message type, if it is routed.
func (b *Bus) transport(typ reflect.Type) (Transport, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.transports[typ]
	return t, ok
}

// typesFor returns the types routed to t, sorted by Go name so that a Decoder
// sees them in a stable order.
func (b *Bus) typesFor(t Transport) []Type {
	b.mu.RLock()
	var types []Type
	for typ, other := range b.transports {
		if other == t {
			types = append(types, b.types[typ])
		}
	}
	b.mu.RUnlock()

	slices.SortFunc(types, func(x, y Type) int { return strings.Compare(x.String(), y.String()) })
	return types
}
