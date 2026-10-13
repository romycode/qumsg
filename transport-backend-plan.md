# Plan: valkey y postgres como implementaciones puras del core

## Prompt para pegar en Claude Code

> Lee `AGENTS.md` entero y después `transport-backend-plan.md`. Ejecuta el plan por fases, un commit por fase, en una rama nueva. Antes de escribir código, contrasta el diseño con el código real: si algo del boceto no encaja, para y explícamelo. No hagas push ni etiquetas sin preguntarme. No hace falta mantener compatibilidad: la API puede romperse.

## Objetivo

Hoy `valkey` y `postgres` hacen dos cosas cada uno:

1. **Traducir**: convertir un `Envelope` a bytes y volver, y recordar qué tipos puede decodificar.
2. **Guardar y leer** en su backend.

La parte 1 está duplicada. Se detectó copia idéntica de `Accept` (~40 líneas), `nameOf`, `Codec`, `jsonCodec`, `UndecodableMessage`, `encodeMetadata`/`decodeMetadata` y el mapa `decoders`, además de dos sentinelas para lo mismo (`valkey.ErrUnnamedMessage` y `postgres.ErrUnnamedMessage`).

**Meta:** la traducción vive una sola vez en el core. `valkey` y `postgres` solo implementan un contrato pequeño de almacenamiento. Un tercer backend debe poder escribirse sin copiar nada de los otros dos.

## Decisiones ya tomadas

- Se puede romper la API pública. No hay alias de compatibilidad.
- `memory.Transport` **no** pasa por el adaptador: serializar en memoria es coste puro. Sigue siendo un `Transport` directo.
- Mantener el formato en el cable (campos `name`, `payload`, `attempt`, `metadata`; columnas de postgres) salvo que haya un motivo. Es barato y evita invalidar mensajes en vuelo.

## Diseño propuesto (boceto, los nombres son orientativos)

El core define un contrato de bytes y un único adaptador que implementa `Transport` y `Decoder`:

```go
// Codec codifica el payload. Por defecto JSON.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// Record es la forma codificada de un Envelope.
type Record struct {
	Name     string            // alias de NamedMessage
	Payload  []byte
	Metadata map[string]string // sin serializar; nil si no hay
}

// Backend guarda y devuelve Records.
type Backend interface {
	// Store recibe el Envelope original y el ctx, además del Record.
	Store(ctx context.Context, e Envelope, r Record) error
	Fetch(ctx context.Context) (RawDelivery, error)
	Close() error
}

type RawDelivery interface {
	ID() string
	Record() Record
	Attempt() int
	Ack(ctx context.Context) error
	Nack(ctx context.Context) error
	Release(ctx context.Context) error
	Context(parent context.Context) (context.Context, context.CancelFunc)
}

// Wire adapta un Backend a Transport y Decoder.
func NewWire(b Backend, opts ...WireOption) (*Wire, error)
```

El adaptador `Wire` se encarga de:

- Validar el mensaje al enviar: `ErrNilMessage`, `ErrUnnamedMessage`, `ErrNamePanic`, `ErrEmptyName`, y `ErrTransportClosed` si está cerrado.
- Codificar con el `Codec` (opción `WithCodec` en el core, no en los backends).
- Implementar `Accept`: lógica de todo-o-nada, alias único por transporte, decodificación con `Type.Decode`.
- Al recibir, decodificar. Si el nombre no está aceptado o el payload es inválido, entregar un `qumsg.UndecodableMessage` (un solo tipo, con `ID string`, `Name`, `Payload`, `Err`).

### Por qué el contrato es así (no cambiar sin pensarlo)

1. **`Store` recibe `Envelope` y `ctx`, no solo bytes.** PostgreSQL lee `Deduplicated` del mensaje, `ContextWithDelay` del contexto y la transacción de `WithDB` también del contexto. Con solo bytes se pierden la deduplicación, el retraso y el envío transaccional.
2. **Los metadatos llegan como `map[string]string`.** Valkey los guarda como string JSON (`""` si no hay) y PostgreSQL como `jsonb`. Cada backend decide cómo.
3. **`Context` y `Release` pasan a formar parte del contrato de entrega.** Hoy `Consume` hace `d.(DeliveryContext)` y `d.(ReleasableDelivery)`, y un wrapper no puede implementarlas condicionalmente. Se eliminan esas interfaces opcionales y esas type assertions en `settle`. Valores por defecto en el core: `Context` devuelve el padre con un cancel vacío; `Release` hace `Nack`. `memory` debe implementarlas con esos valores por defecto.
4. **`Propagator`, `Complete` y `Own` de postgres se quedan en el backend.** El usuario conserva el objeto backend para ellos y pasa el `Wire` al router. La entrada de metadatos `qumsg.postgres.lease` debe atravesar el adaptador sin cambios (test obligatorio).

Uso final esperado:

```go
b, _ := postgres.New(pool, "messages", postgres.WithOwner("worker-1"))
t, _ := qumsg.NewWire(b)
router.Route[SendWelcomeEmail](t)
bus := qumsg.New(qumsg.WithRouter(router), qumsg.WithPropagators(b.Propagator()))
```

## Fases (un commit por fase, Conventional Commits en inglés)

### Fase 0: línea base
- Rama nueva desde `main`. Ejecutar `make check` y anotar el resultado antes de tocar nada.
- Leer `transport.go`, `router.go`, `types.go`, `envelope.go`, `memory/memory.go`, `valkey/valkey.go`, `postgres/postgres.go` y sus tests. Confirmar o corregir el boceto.

### Fase 1: contrato de entrega en el core
- `Context` y `Release` entran en `Delivery`. Se borran `DeliveryContext`, `ReleasableDelivery` y sus type assertions en `settle`.
- Adaptar `memory` (valores por defecto) y los tests de `transport_test.go`/`lifecycle_test.go`.
- Commit: `refactor!: fold DeliveryContext and ReleasableDelivery into Delivery`.

### Fase 2: capa de cable en el core
- Añadir `Codec`, `Record`, `Backend`, `RawDelivery`, `Wire`, `WireOption` (`WithCodec`), `UndecodableMessage`, `ErrUnnamedMessage` y `nameOf`.
- Tests unitarios con un `Backend` falso en el propio test (usar `testing/synctest`, nunca `time.Sleep`). Deben cubrir: todo-o-nada de `Accept`, alias duplicado, mensaje sin nombre, `Name()` que entra en pánico, payload inválido → `UndecodableMessage`, cierre, y ida y vuelta de metadatos.
- `example_test.go` + sección en `README.md`.
- Commit: `feat!: add Backend and Wire to share the wire layer between transports`.

### Fase 3: valkey
- Convertir `valkey.Transport` en un `Backend`. Borrar `Accept`, `nameOf`, `Codec`, `jsonCodec`, `UndecodableMessage`, `ErrUnnamedMessage` y el mapa `decoders`.
- Quitar `WithCodec` (pasa al core). Mantener `XAUTOCLAIM`, `XPENDING`, `WithMaxLen`, `WithBlock`, `WithClaimIdle`.
- Adaptar `valkey_test.go`, `integration_test.go`, `example_test.go` y `valkey/README.md`.
- Commit: `refactor(valkey)!: implement qumsg.Backend only`.

### Fase 4: postgres (la delicada)
- Convertir `postgres.Transport` en un `Backend`. Mismas supresiones que en valkey. El `id` de fila pasa a `string` en `UndecodableMessage` (`strconv`).
- **No tocar** leases, latido, fencing, `LISTEN/NOTIFY`, backoff, `Complete` ni `Own`. Solo cambia la superficie.
- Tests obligatorios: `Deduplicated` sigue deduplicando, `ContextWithDelay` retrasa, el envío con `WithDB` dentro de una transacción se revierte con ella, y el lease llega al `ctx` del handler vía `Propagator()`.
- Adaptar los tests (unitarios con `DB` falso e integración) y `postgres/README.md`.
- Commit: `refactor(postgres)!: implement qumsg.Backend only`.

### Fase 5: documentación
- `CHANGELOG.md` en `[Sin publicar]`, en español, con una línea **Incompatible:** por cada cambio.
- Actualizar `AGENTS.md`: las secciones de arquitectura de `valkey` y `postgres` describen `Accept` y el codec en cada transporte.

### Fase 6: verificación final
- `make check` completo (con Docker para la integración) y `go vet -tags integration`.
- Comprobar con `grep` que en `valkey/` y `postgres/` no quedan `func nameOf`, `Accept(`, `jsonCodec` ni `UndecodableMessage`.
- Los tests de comportamiento previos deben seguir pasando, adaptados solo en la forma de construir el transporte.

## Release (no hacer sin confirmación)

`valkey` y `postgres` requieren el core **publicado**; `go.work` solo lo oculta en local. Según `AGENTS.md` (sección Git, y el precedente de `v0.4.0` en el CHANGELOG), si un módulo anidado usa algo nuevo del núcleo se etiqueta primero el core, y la etiqueta del módulo va en el commit siguiente, el que sube su `require`. Es un cambio incompatible en versión 0.x, así que lo normal es subir la minor. Seguir el procedimiento de `AGENTS.md` y pedir confirmación antes de etiquetar o hacer push.

## Criterios de aceptación

- Un tercer backend se escribe implementando solo `Backend` y `RawDelivery`, sin copiar código de valkey ni de postgres (el test con el `Backend` falso lo demuestra).
- No queda código de traducción duplicado en `valkey/` ni en `postgres/`.
- Deduplicación, retraso, envío transaccional y propagación del lease de postgres funcionan igual que antes.
- `make check` en verde. `README.md`, READMEs de módulo, `AGENTS.md` y `CHANGELOG.md` actualizados.

## Reglas del repo a respetar

- Go 1.27+: métodos genéricos antes que funciones genéricas.
- Documentación y CHANGELOG en español; identificadores, comentarios de código y commits en inglés.
- Errores `Err*` exportados, envueltos con `%w`; los errores de los handlers se devuelven sin envolver.
- Tests unitarios con `synctest`; en integración, esperas cortas y acotadas.
- No tocar `.zzz/`.
