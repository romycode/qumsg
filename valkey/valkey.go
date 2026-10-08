// Package valkey implements a qumsg.Transport on Valkey Streams.
//
// Messages are appended to a stream with XADD and consumed by a consumer group
// with XREADGROUP, so several workers and processes share the work and each
// message is processed by one of them. A message stays pending until it is
// acknowledged; if its consumer stops, another one claims it after an idle
// time. Delivery is at least once.
//
// Messages are identified on the wire by their qumsg.NamedMessage alias, not
// by their Go type, so types can be renamed or moved without breaking queued
// messages. The consumer side declares the types it can decode with
// Transport.Register.
package valkey

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/romycode/qumsg"
	vk "github.com/valkey-io/valkey-go"
)

// Fields of a stream entry.
const (
	fieldName    = "name"
	fieldPayload = "payload"
	fieldAttempt = "attempt"
)

// ErrUnnamedMessage is returned by Send and Register when the message type
// does not implement qumsg.NamedMessage, which is required to identify it on
// the wire.
var ErrUnnamedMessage = errors.New("valkey: message type must implement qumsg.NamedMessage")

// Codec encodes message payloads. The default is JSON.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// UndecodableMessage is delivered instead of a message whose name is not
// registered or whose payload cannot be decoded. No handler is usually
// registered for it, so it fails with qumsg.ErrNoHandler, exhausts its
// attempts and reaches the FailureHandler of Consume.
type UndecodableMessage struct {
	ID      string
	Name    string
	Payload []byte
	Err     error
}

// Option configures a Transport.
type Option func(*Transport)

// WithGroup sets the consumer group. Default "qumsg".
func WithGroup(name string) Option {
	return func(t *Transport) { t.group = name }
}

// WithConsumer sets the consumer name within the group. It must be unique per
// process. Default: hostname plus a random suffix.
func WithConsumer(name string) Option {
	return func(t *Transport) { t.consumer = name }
}

// WithBlock sets how long each XREADGROUP waits for new messages. Receive
// notices a canceled context or Close within this time. Default 1s.
func WithBlock(d time.Duration) Option {
	if d < time.Millisecond {
		panic("valkey: WithBlock requires d >= 1ms")
	}
	return func(t *Transport) { t.block = d }
}

// WithClaimIdle sets how long a message must stay pending, without being
// acknowledged, before another consumer claims it. It must be longer than the
// slowest handler, or messages still in progress are processed twice.
// Default 1m.
func WithClaimIdle(d time.Duration) Option {
	if d < time.Millisecond {
		panic("valkey: WithClaimIdle requires d >= 1ms")
	}
	return func(t *Transport) { t.claimIdle = d }
}

// WithMaxLen trims the stream to about n entries on every XADD. Trimming can
// remove messages not yet consumed if the backlog grows beyond n. Default: no
// trimming.
func WithMaxLen(n int64) Option {
	if n < 1 {
		panic("valkey: WithMaxLen requires n >= 1")
	}
	return func(t *Transport) { t.maxLen = n }
}

// WithCodec sets the payload codec. Default JSON.
func WithCodec(c Codec) Option {
	return func(t *Transport) { t.codec = c }
}

// Transport is a qumsg.Transport on a Valkey stream, safe for concurrent use.
// It does not own the client: close the client after the transport.
type Transport struct {
	client    vk.Client
	stream    string
	group     string
	consumer  string
	block     time.Duration
	claimIdle time.Duration
	maxLen    int64
	codec     Codec

	mu       sync.RWMutex
	decoders map[string]decoder
	types    map[string]reflect.Type

	groupMu    sync.Mutex
	groupReady bool

	closed atomic.Bool
}

type decoder func(codec Codec, payload []byte) (any, error)

var _ qumsg.Transport = (*Transport)(nil)

// New creates a transport on stream.
func New(client vk.Client, stream string, opts ...Option) *Transport {
	t := &Transport{
		client:    client,
		stream:    stream,
		group:     "qumsg",
		consumer:  defaultConsumer(),
		block:     time.Second,
		claimIdle: time.Minute,
		codec:     jsonCodec{},
		decoders:  make(map[string]decoder),
		types:     make(map[string]reflect.Type),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func defaultConsumer() string {
	host, err := os.Hostname()
	if err != nil {
		host = "qumsg"
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return host + "-" + hex.EncodeToString(suffix)
}

// Register declares that messages of type T can be received. T must implement
// qumsg.NamedMessage with the same rules as qumsg.Bus.Register: a non-empty
// alias, unique per transport.
func (t *Transport) Register[T any]() error {
	typ := reflect.TypeFor[T]()
	if typ.Kind() == reflect.Interface {
		return fmt.Errorf("%w: %s", qumsg.ErrInterfaceType, typ)
	}

	name, err := messageName[T]()
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if other, ok := t.types[name]; ok && other != typ {
		return fmt.Errorf("%w: %q is used by %s, cannot register %s", qumsg.ErrDuplicateName, name, other, typ)
	}
	t.types[name] = typ
	t.decoders[name] = func(codec Codec, payload []byte) (any, error) {
		var msg T
		if err := codec.Unmarshal(payload, &msg); err != nil {
			return nil, err
		}
		return msg, nil
	}

	return nil
}

// messageName returns the alias of T. For pointer types it calls Name on a
// pointer to a zero value, like qumsg.Bus.Register.
func messageName[T any]() (string, error) {
	typ := reflect.TypeFor[T]()

	var sample T
	if typ.Kind() == reflect.Pointer {
		sample = reflect.New(typ.Elem()).Interface().(T)
	}

	named, ok := any(sample).(qumsg.NamedMessage)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnnamedMessage, typ)
	}
	return nameOf(named)
}

// nameOf calls Name, turning a panic into qumsg.ErrNamePanic.
func nameOf(named qumsg.NamedMessage) (name string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %T: %v", qumsg.ErrNamePanic, named, r)
		}
	}()

	name = named.Name()
	if name == "" {
		return "", fmt.Errorf("%w: %T", qumsg.ErrEmptyName, named)
	}
	return name, nil
}

// Send appends message to the stream. The message type must implement
// qumsg.NamedMessage; it does not need to be registered.
func (t *Transport) Send(ctx context.Context, message any) error {
	if t.closed.Load() {
		return qumsg.ErrTransportClosed
	}
	if message == nil {
		return qumsg.ErrNilMessage
	}

	named, ok := message.(qumsg.NamedMessage)
	if !ok {
		return fmt.Errorf("%w: %T", ErrUnnamedMessage, message)
	}
	name, err := nameOf(named)
	if err != nil {
		return err
	}

	payload, err := t.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("valkey: encode %s: %w", name, err)
	}

	return t.client.Do(ctx, t.xadd(name, string(payload), 1)).Error()
}

func (t *Transport) xadd(name, payload string, attempt int) vk.Completed {
	// valkey-go does not export the intermediate builder types, so both
	// branches build the whole command.
	key := t.client.B().Xadd().Key(t.stream)
	attemptStr := strconv.Itoa(attempt)
	if t.maxLen > 0 {
		return key.Maxlen().Almost().Threshold(strconv.FormatInt(t.maxLen, 10)).Id("*").FieldValue().
			FieldValue(fieldName, name).
			FieldValue(fieldPayload, payload).
			FieldValue(fieldAttempt, attemptStr).
			Build()
	}
	return key.Id("*").FieldValue().
		FieldValue(fieldName, name).
		FieldValue(fieldPayload, payload).
		FieldValue(fieldAttempt, attemptStr).
		Build()
}

// Receive returns the next message. It first claims messages that other
// consumers left pending for longer than the claim idle time, then reads new
// messages, waiting up to the block time each round. It returns when ctx is
// done or after Close (qumsg.ErrTransportClosed); queued messages stay in the
// stream.
func (t *Transport) Receive(ctx context.Context) (qumsg.Delivery, error) {
	for {
		if t.closed.Load() {
			return nil, qumsg.ErrTransportClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := t.ensureGroup(ctx); err != nil {
			return nil, err
		}

		d, err := t.claim(ctx)
		if err != nil || d != nil {
			return d, err
		}

		d, err = t.read(ctx)
		if err != nil || d != nil {
			return d, err
		}
	}
}

// ensureGroup creates the consumer group once. It reads the stream from the
// beginning, so messages sent before any consumer started are not lost.
func (t *Transport) ensureGroup(ctx context.Context) error {
	t.groupMu.Lock()
	defer t.groupMu.Unlock()
	if t.groupReady {
		return nil
	}

	cmd := t.client.B().XgroupCreate().Key(t.stream).Group(t.group).Id("0").Mkstream().Build()
	if err := t.client.Do(ctx, cmd).Error(); err != nil && !vk.IsValkeyBusyGroup(err) {
		return fmt.Errorf("valkey: create group %s: %w", t.group, err)
	}
	t.groupReady = true
	return nil
}

// claim takes over one message pending for longer than the claim idle time.
func (t *Transport) claim(ctx context.Context) (qumsg.Delivery, error) {
	cmd := t.client.B().Xautoclaim().Key(t.stream).Group(t.group).Consumer(t.consumer).
		MinIdleTime(strconv.FormatInt(t.claimIdle.Milliseconds(), 10)).Start("0-0").Count(1).Build()
	res, err := t.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, fmt.Errorf("valkey: claim: %w", err)
	}
	if len(res) < 2 {
		return nil, fmt.Errorf("valkey: claim: unexpected reply of %d elements", len(res))
	}
	entries, err := res[1].AsXRange()
	if err != nil {
		return nil, fmt.Errorf("valkey: claim: %w", err)
	}
	if len(entries) == 0 {
		return nil, nil
	}

	entry := entries[0]
	deliveries, err := t.deliveryCount(ctx, entry.ID)
	if err != nil {
		return nil, err
	}
	// The entry attempt counts its first delivery; every extra delivery of
	// the same entry, such as this claim, is another attempt.
	return t.delivery(entry, deliveries-1), nil
}

// deliveryCount returns how many times the pending entry id was delivered.
func (t *Transport) deliveryCount(ctx context.Context, id string) (int, error) {
	cmd := t.client.B().Xpending().Key(t.stream).Group(t.group).Start(id).End(id).Count(1).Build()
	res, err := t.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return 0, fmt.Errorf("valkey: pending %s: %w", id, err)
	}
	if len(res) == 0 {
		return 1, nil
	}
	info, err := res[0].ToArray()
	if err != nil || len(info) < 4 {
		return 0, fmt.Errorf("valkey: pending %s: unexpected reply: %v", id, err)
	}
	n, err := info[3].AsInt64()
	if err != nil {
		return 0, fmt.Errorf("valkey: pending %s: %w", id, err)
	}
	return int(n), nil
}

// read waits up to the block time for a new message.
func (t *Transport) read(ctx context.Context) (qumsg.Delivery, error) {
	// valkey-go only interrupts a blocking command on a deadline, not on a
	// manual cancel, so bound the call slightly beyond the block time.
	callCtx, cancel := context.WithTimeout(ctx, t.block+5*time.Second)
	defer cancel()

	cmd := t.client.B().Xreadgroup().Group(t.group, t.consumer).Count(1).
		Block(t.block.Milliseconds()).Streams().Key(t.stream).Id(">").Build()
	res, err := t.client.Do(callCtx, cmd).AsXRead()
	switch {
	case vk.IsValkeyNil(err):
		return nil, nil
	case err != nil:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("valkey: read: %w", err)
	}

	entries := res[t.stream]
	if len(entries) == 0 {
		return nil, nil
	}
	return t.delivery(entries[0], 0), nil
}

// delivery decodes an entry. extra is the number of deliveries of this entry
// beyond the first one.
func (t *Transport) delivery(entry vk.XRangeEntry, extra int) *delivery {
	name := entry.FieldValues[fieldName]
	payload := []byte(entry.FieldValues[fieldPayload])

	attempt, err := strconv.Atoi(entry.FieldValues[fieldAttempt])
	if err != nil || attempt < 1 {
		attempt = 1
	}

	d := &delivery{
		transport: t,
		id:        entry.ID,
		name:      name,
		payload:   payload,
		attempt:   attempt + extra,
	}

	t.mu.RLock()
	decode, ok := t.decoders[name]
	t.mu.RUnlock()

	if !ok {
		d.message = UndecodableMessage{ID: entry.ID, Name: name, Payload: payload,
			Err: fmt.Errorf("valkey: no type registered for %q", name)}
		return d
	}

	msg, err := decode(t.codec, payload)
	if err != nil {
		d.message = UndecodableMessage{ID: entry.ID, Name: name, Payload: payload,
			Err: fmt.Errorf("valkey: decode %s: %w", name, err)}
		return d
	}
	d.message = msg
	return d
}

// Close makes Receive and Send return qumsg.ErrTransportClosed. Messages in
// the stream are kept. A Receive in progress returns within the block time.
// It does not close the client.
func (t *Transport) Close() error {
	t.closed.Store(true)
	return nil
}

type delivery struct {
	transport *Transport
	id        string
	name      string
	payload   []byte
	message   any
	attempt   int
	settled   atomic.Bool
}

func (d *delivery) Message() any { return d.message }

func (d *delivery) Attempt() int { return d.attempt }

// Ack acknowledges the entry, removing it from the pending list.
func (d *delivery) Ack(ctx context.Context) error {
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}
	t := d.transport
	cmd := t.client.B().Xack().Key(t.stream).Group(t.group).Id(d.id).Build()
	if err := t.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey: ack %s: %w", d.id, err)
	}
	return nil
}

// Nack appends the message again with the next attempt number and then
// acknowledges the original entry. The retry is delivered immediately. If
// the process stops between both commands, the message is delivered twice.
func (d *delivery) Nack(ctx context.Context) error {
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}
	t := d.transport

	if err := t.client.Do(ctx, t.xadd(d.name, string(d.payload), d.attempt+1)).Error(); err != nil {
		return fmt.Errorf("valkey: retry %s: %w", d.id, err)
	}
	cmd := t.client.B().Xack().Key(t.stream).Group(t.group).Id(d.id).Build()
	if err := t.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey: ack %s: %w", d.id, err)
	}
	return nil
}
