package qumsg

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
)

var (
	// ErrAlreadyRouted is returned by Router.Route and Router.RouteTypes when
	// the message type is already routed to another transport.
	ErrAlreadyRouted = errors.New("qumsg: message type already routed to another transport")
	// ErrNoRoutes is returned by Consume when the transport is a Decoder and
	// the router of the bus routes no message type to it, so it could not
	// decode anything.
	ErrNoRoutes = errors.New("qumsg: no message types routed to the transport")
)

// Router decides which messages are also sent to a transport when they are
// dispatched. Each message type goes to at most one transport.
//
// Share the same Router between the process that sends and the worker that
// consumes: a bus created WithRouter sends the routed messages it dispatches,
// and Consume handles the messages it receives with the local handlers,
// without sending them again, and tells a Decoder transport which types it
// carries.
//
// Configure the routes before dispatching; a Router is safe for concurrent
// use.
type Router struct {
	mu     sync.RWMutex
	routes map[reflect.Type]Transport
	types  map[reflect.Type]Type
	names  map[string]reflect.Type
}

// NewRouter returns a router without routes.
func NewRouter() *Router {
	return &Router{
		routes: make(map[reflect.Type]Transport),
		types:  make(map[reflect.Type]Type),
		names:  make(map[string]reflect.Type),
	}
}

// Route sends messages of the exact type T to t. T follows the rules of
// Bus.Register: it must be concrete and its alias, if any, must be valid and
// unique within the router.
//
//	router.Route[SendWelcomeEmail](transport)
func (r *Router) Route[T any](t Transport) error {
	return r.RouteTypes(t, TypeOf[T]())
}

// RouteTypes is Route for every type in types. It routes all of them or, if
// any is invalid, already routed to another transport or uses a taken alias,
// none.
func (r *Router) RouteTypes(t Transport, types ...Type) error {
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

	r.mu.Lock()
	defer r.mu.Unlock()

	pending := make(map[string]reflect.Type)
	for _, typ := range types {
		if other, ok := r.routes[typ.typ]; ok && other != t {
			return fmt.Errorf("%w: %s", ErrAlreadyRouted, typ)
		}
		name, named := typ.Name()
		if !named {
			continue
		}
		owner, ok := r.names[name]
		if !ok {
			owner, ok = pending[name]
		}
		if ok && owner != typ.typ {
			return fmt.Errorf("%w: %q is used by %s, cannot route %s", ErrDuplicateName, name, owner, typ)
		}
		pending[name] = typ.typ
	}
	for _, typ := range types {
		r.routes[typ.typ] = t
		r.types[typ.typ] = typ
		if name, named := typ.Name(); named {
			r.names[name] = typ.typ
		}
	}
	return nil
}

// transport returns the transport of the message type, if it is routed.
func (r *Router) transport(typ reflect.Type) (Transport, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.routes[typ]
	return t, ok
}

// typesFor returns the types routed to t, sorted by Go name so that a Decoder
// sees them in a stable order.
func (r *Router) typesFor(t Transport) []Type {
	r.mu.RLock()
	var types []Type
	for typ, other := range r.routes {
		if other == t {
			types = append(types, r.types[typ])
		}
	}
	r.mu.RUnlock()

	slices.SortFunc(types, func(x, y Type) int { return strings.Compare(x.String(), y.String()) })
	return types
}

// WithRouter makes the bus send the messages routed by r to their transport
// after running their local handlers, if any; see Dispatch. It panics if r is
// nil.
func WithRouter(r *Router) Option {
	if r == nil {
		panic("qumsg: nil router")
	}
	return func(b *Bus) { b.router = r }
}
