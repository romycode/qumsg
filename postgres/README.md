# qumsg/postgres

Transporte de [qumsg](../README.md) sobre una tabla de PostgreSQL. Permite enviar mensajes dentro de la misma transacción que la escritura que los necesita (el mensaje existe si y solo si esa transacción se confirma) y consumirlos desde uno o varios procesos, con leases que se renuevan mientras dura el handler, reintentos con espera, mensajes únicos por clave y recuperación de los mensajes de un worker caído.

Es un módulo aparte para que el núcleo de `qumsg` no dependa de pgx:

```sh
go get github.com/romycode/qumsg/postgres
```

Usa [`pgx/v5`](https://github.com/jackc/pgx) (`*pgxpool.Pool`). Requiere Go 1.27+ y PostgreSQL 12+.

## Uso

Los mensajes que viajan por PostgreSQL deben implementar `qumsg.NamedMessage`. El alias es su identificador en la tabla, así que puedes renombrar o mover el tipo Go sin romper los mensajes ya encolados:

```go
type SendWelcomeEmail struct {
    Email string
}

func (SendWelcomeEmail) Name() string {
    return "emails.welcome"
}
```

Los dos procesos comparten las rutas: una función que crea el router para el transporte de cada proceso.

```go
func routes(t qumsg.Transport) *qumsg.Router {
    router := qumsg.NewRouter()
    _ = router.Route[SendWelcomeEmail](t)
    return router
}
```

El proceso productor inserta el mensaje en la tabla:

```go
pool, err := pgxpool.New(ctx, "postgres://localhost:5432/app")
if err != nil {
    return err
}
defer pool.Close()

transport, err := postgres.New(pool, "email_messages")
if err != nil {
    return err
}

producer := qumsg.New(qumsg.WithRouter(routes(transport)))
err = producer.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})
```

El proceso worker consume la tabla con sus handlers:

```go
transport, err := postgres.New(pool, "email_messages")
if err != nil {
    return err
}
defer transport.Close()

worker := qumsg.New(qumsg.WithRouter(routes(transport)))
if err := worker.Register(sendWelcomeEmail); err != nil {
    return err
}

return worker.Consume(ctx, transport, qumsg.WithWorkers(4), qumsg.WithMaxAttempts(5))
```

El transporte implementa `qumsg.Decoder`: antes de recibir, `Consume` le pasa con `Accept` los tipos que el router envía a ese transporte, y así sabe decodificar cada alias. Esos tipos siguen las mismas reglas que `qumsg.Bus.Register`: el alias no puede estar vacío ni repetirse entre tipos. Si alguno no es válido, `Consume` devuelve el error sin empezar a recibir.

`Close` libera la conexión de `LISTEN` del transporte; llámalo cuando `Consume` haya terminado y antes de cerrar el pool.

## La tabla

La tabla la crea la aplicación en su propia migración. `postgres.Schema("email_messages")` devuelve exactamente estas sentencias, con el nombre de la tabla sustituido:

```sql
CREATE TABLE "email_messages"
(
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text        NOT NULL,
    payload      bytea       NOT NULL,
    metadata     jsonb       NOT NULL DEFAULT '{}',
    dedup_key    text UNIQUE,
    attempt      integer     NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_by    text,
    locked_at    timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX "email_messages_ready" ON "email_messages" (available_at, id) WHERE locked_at IS NULL;
CREATE INDEX "email_messages_locked" ON "email_messages" (locked_at) WHERE locked_at IS NOT NULL;
```

| Columna | Contenido |
| --- | --- |
| `name` | El alias (`Name()`) del mensaje. |
| `payload` | El mensaje codificado por el codec (JSON por defecto). |
| `metadata` | Los metadatos del `qumsg.Envelope` como objeto JSON (`{}` si no hay); no depende del codec. |
| `dedup_key` | La clave de `postgres.Deduplicated`, o `NULL`. |
| `attempt` | Cuántas veces se ha entregado el mensaje. |
| `available_at` | Desde cuándo se puede entregar (la primera vez o tras un `Nack`). |
| `locked_by`, `locked_at` | Dueño del lease y su última renovación; `NULL` si nadie lo tiene. |

El nombre de la tabla debe ser un identificador en minúsculas (`[a-z_][a-z0-9_]*`) de 56 caracteres como mucho, porque se inserta en el SQL y da nombre a los índices y al canal de notificaciones (`qumsg_<tabla>`); si no, `New` y `Schema` devuelven `postgres.ErrInvalidTable`. Puedes añadir columnas o claves foráneas propias (p. ej. con `ON DELETE CASCADE`), siempre que tengan valor por defecto.

## Enviar dentro de una transacción

`Send` usa el pool por defecto. Con `WithDB` usa otro `postgres.DB` (`Exec` y `QueryRow` con las firmas de pgx/v5), y lo habitual es pasar un ejecutor que, en cada consulta, use la transacción que lleve el `ctx` o, si no hay, el pool. El `INSERT` y su `NOTIFY` van entonces en la transacción de la aplicación: si se revierte, el mensaje no existe; si se confirma, PostgreSQL entrega la notificación en ese momento y los workers lo reciben al instante.

```go
// En la aplicación: un ejecutor que toma la transacción del contexto.
type txKey struct{}

type Executor struct{ pool *pgxpool.Pool }

func (e Executor) db(ctx context.Context) postgres.DB {
    if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
        return tx
    }
    return e.pool
}

func (e Executor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
    return e.db(ctx).Exec(ctx, sql, args...)
}

func (e Executor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
    return e.db(ctx).QueryRow(ctx, sql, args...)
}

func (e Executor) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
    return pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
        return fn(context.WithValue(ctx, txKey{}, tx))
    })
}
```

```go
executor := Executor{pool: pool}
transport, err := postgres.New(pool, "email_messages", postgres.WithDB(executor))

err = executor.WithinTransaction(ctx, func(ctx context.Context) error {
    if _, err := executor.Exec(ctx, `INSERT INTO users (email) VALUES ($1)`, email); err != nil {
        return err
    }
    // Se inserta en la misma transacción: se confirma o se revierte con el usuario.
    return bus.Dispatch(ctx, SendWelcomeEmail{Email: email})
})
```

`Receive`, `Ack`, `Nack` y la renovación del lease siempre usan el pool, en sentencias cortas propias: nunca participan en la transacción de quien envía.

## Terminar el mensaje en la transacción del handler

Por defecto, el trabajo del handler y el `Ack` que `Consume` hace después **no** son una transacción: si el proceso cae entre los dos, el mensaje se entrega otra vez. Cuando la escritura final del handler debe ser atómica con el fin del mensaje, el handler llama a `Transport.Complete` dentro de su transacción:

```go
worker := qumsg.New(
    qumsg.WithRouter(routes(transport)),
    qumsg.WithPropagators(transport.Propagator()), // lleva el lease al ctx del handler
)

_ = worker.Register(func(ctx context.Context, msg VerifyResource) error {
    return executor.WithinTransaction(ctx, func(ctx context.Context) error {
        if err := markReady(ctx, msg.ResourceID); err != nil {
            return err
        }
        // Borra el mensaje en esta transacción, si el lease sigue siendo nuestro.
        return transport.Complete(ctx, executor)
    })
})
```

`Complete` borra la fila cercada por su lease (token de reclamación e intento). Si otro worker la ha reclamado devuelve `postgres.ErrLeaseLost` y no borra nada: devuelve el error para que la transacción se revierta. Sin `Propagator` en el bus, o fuera de un handler, devuelve `postgres.ErrNoLease`. Tras confirmarse, el `Ack` de `Consume` no encuentra la fila y no hace nada.

`Own(ctx, db)` comprueba el mismo lease y bloquea la fila con `FOR UPDATE` sin borrarla. Úsalo al principio de una transacción corta que registra efectos parciales, antes de realizar esos efectos fuera de la base de datos. Así, una entrega antigua no puede registrar nuevos efectos después de que otra la reclame. El bloqueo dura hasta terminar la transacción. No mantengas transacciones abiertas durante el procesado multimedia o las llamadas remotas: si una renovación encuentra la fila bloqueada, cancela conservadoramente el contexto del handler.

`Propagator` funciona con los metadatos del sobre: el transporte añade a cada entrega la entrada `qumsg.postgres.lease` y el propagador la pasa al `ctx`. Su `Inject` no hace nada, así que el lease nunca viaja a los mensajes que el handler despache.

## Mensajes únicos

Un mensaje que implementa `postgres.Deduplicated` lleva una clave en `dedup_key`, que es `UNIQUE`. Mientras haya en la tabla un mensaje con la misma clave (esperando, en curso o esperando un reintento), `Send` de otro no hace nada y devuelve `nil`: el mensaje existente gana, con su payload y sus metadatos. Una clave vacía no deduplica.

```go
type VerifyResource struct {
    ResourceID string
}

func (VerifyResource) Name() string { return "resources.verify" }

func (m VerifyResource) DedupKey() string { return "verify:" + m.ResourceID }
```

Cuando el mensaje se confirma (o agota sus intentos y llega al `FailureHandler`), la fila se borra y la clave queda libre.

## Envío diferido

`postgres.ContextWithDelay(ctx, d)` hace que `Send` guarde el mensaje con `available_at = now() + d`, con el reloj de la base de datos. Afecta a todo lo que el bus envíe a un transporte de PostgreSQL con ese `ctx`:

```go
err := bus.Dispatch(postgres.ContextWithDelay(ctx, time.Hour), RemoveObject{Key: key})
```

## Cómo funciona

| Operación | SQL |
| --- | --- |
| `Send` | `INSERT … ON CONFLICT (dedup_key) DO NOTHING` y, si se insertó, `pg_notify('qumsg_<tabla>', '')`, con el `DB` de `WithDB`. |
| `Receive` | Un `UPDATE … SET locked_by, locked_at = now(), attempt = attempt + 1 WHERE id = COALESCE(<lease caducado>, <disponible más antiguo>)`, cada subconsulta con `FOR UPDATE SKIP LOCKED`, en autocommit. Si no hay nada, espera (ver abajo). |
| Renovación | `UPDATE … SET locked_at = now()` cercado y con `FOR UPDATE NOWAIT`, cada tercio del lease. |
| `Ack` | `DELETE` cercado. |
| `Nack` | `UPDATE` cercado que libera el lease con `available_at = now() + backoff(attempt)`, y `pg_notify`. |
| `Complete` | `DELETE` cercado con el `DB` del handler. |

- **Reparto.** `FOR UPDATE SKIP LOCKED` hace que dos workers nunca reclamen la misma fila: cada mensaje lo procesa un solo worker mientras su lease esté vigente.
- **Leases y latido.** Cada entrega guarda un token único (`WithOwner` más un sufijo aleatorio por reclamación), hora e intento. Mientras no se liquida, una goroutine renueva `locked_at` cada tercio de `WithLease`. Una renovación fallida o sin fila cancela el contexto del handler con `ErrLeaseLost`. Una fila bloqueada (`55P03`, normalmente la transacción `Own`/`Complete` del propio handler) no cuenta como renovación ni mueve el límite seguro, pero tampoco cancela: la renovación se reintenta cada sexto del lease (mínimo 10 ms) sin esperar al siguiente tick, y un éxito mueve el límite seguro como cualquier renovación. Solo se cancela si se agota el límite seguro, porque la transacción podría revertirse. El deadline se calcula desde antes de la petición y deja un tercio del lease de margen. La liquidación detiene y espera al latido.
- **Workers caídos.** Si un worker deja de renovar (se cae, se cuelga o pierde la base de datos) durante más de `WithLease`, otro `Receive` reclama el mensaje y cuenta esa entrega como un intento más, así que un mensaje que tumba al proceso acaba llegando al `FailureHandler`.
- **Fencing.** `Ack`, `Nack`, `Release`, `Own`, la renovación y `Complete` solo actúan si `locked_by` y `attempt` siguen siendo los de la entrega. El token cambia incluso cuando `Release` descuenta el intento. Si otro worker tiene el mensaje, devuelven `postgres.ErrLeaseLost` sin cambiar nada; si la fila ya no existe, `Ack`, `Nack` y `Release` devuelven `nil`.
- **Reintentos e intentos.** `Attempt()` es el número de entregas, empezando en 1. `Consume` hace `Nack` mientras `Attempt() < WithMaxAttempts` (3 por defecto); el mensaje vuelve a estar disponible tras `WithBackoff(attempt)` (por defecto 1 s, doblando hasta 5 min). En el último intento, `Consume` llama al `FailureHandler` y hace `Ack`, que borra la fila.
- **Espera con LISTEN/NOTIFY.** El primer `Receive` abre una conexión propia, fuera del pool y con su configuración, y hace `LISTEN qumsg_<tabla>` hasta `Close`. Un `Receive` sin nada que reclamar espera a una notificación, a que venza el próximo `available_at`, a que caduque el lease más antiguo o a `WithPollInterval` (5 s), lo que llegue antes. Si la conexión de `LISTEN` falla, se reintenta cada segundo y entretanto `Receive` sondea. Debe llegar a PostgreSQL directamente o a través de un pooler en modo sesión: PgBouncer en modo transacción no mantiene `LISTEN`.
- **Apagado.** Por defecto se termina el trabajo en curso. Con `qumsg.WithCancelOnShutdown()` se cancela el handler; si devuelve error, `Release` libera el mensaje inmediatamente y descuenta el intento. La liquidación usa un contexto separado limitado a cinco segundos. `qumsg.WithUnlimitedAttempts()` permite reintentar limpieza sin límite y `qumsg.Attempt(ctx)` permite aplicar una política terminal en el propio handler y su transacción.
- **Entrega al menos una vez.** Si un proceso cae después del handler y antes del `Ack`, o pierde el lease, el mensaje se procesará otra vez. Los handlers deben tolerar duplicados, o terminar el mensaje con `Complete` en su transacción.
- **Mensajes que no se pueden decodificar.** Si el alias no está aceptado o el payload o los metadatos no se pueden decodificar, se entrega un `postgres.UndecodableMessage` con el id de la fila, el nombre, el payload y el error. Normalmente no tiene handler, así que falla con `qumsg.ErrNoHandler`, agota sus intentos y llega al `FailureHandler` de `Consume`.

## Opciones

| Opción | Por defecto | Qué controla |
| --- | --- | --- |
| `WithDB(db)` | el pool | Con qué ejecuta `Send` (y `Complete` con `db` nil): un ejecutor que use la transacción del `ctx`. |
| `WithOwner(name)` | hostname + sufijo aleatorio | Dueño de los leases; único por proceso. |
| `WithLease(d)` | `1m` | Cuánto dura un lease sin renovar. Se renueva cada `d/3`. Mínimo 30 ms. |
| `WithBackoff(f)` | `DefaultBackoff` | Espera tras un `Nack` según el intento fallido. |
| `WithPollInterval(d)` | `5s` | Espera máxima de un `Receive` ocioso sin notificación. |
| `WithCodec(c)` | JSON | Codificación del payload. |

## Errores

Además de los errores de PostgreSQL, que se devuelven envueltos con la operación:

| Error | Cuándo |
| --- | --- |
| `postgres.ErrInvalidTable` | `New` o `Schema` con un nombre de tabla no válido. |
| `postgres.ErrUnnamedMessage` | `Send` o `Accept` (al empezar `Consume`) con un tipo que no implementa `qumsg.NamedMessage`. |
| `postgres.ErrLeaseLost` | `Ack`, `Nack` o `Complete` de una entrega cuyo mensaje tiene ahora otro worker. |
| `postgres.ErrNoLease` | `Complete` con un `ctx` sin lease del transporte. |
| `qumsg.ErrEmptyName`, `qumsg.ErrNamePanic`, `qumsg.ErrDuplicateName`, `qumsg.ErrInterfaceType` | Las mismas validaciones del alias que hace `qumsg.Bus.Register`. |
| `qumsg.ErrNilMessage` | `Send` con un sobre sin mensaje. |
| `qumsg.ErrTransportClosed` | `Send` o `Receive` después de `Close`. `Close` no borra mensajes ni cierra el pool. |

`postgres.ErrLeaseLost` envuelve `qumsg.ErrDeliveryLost`: `Consume` no detiene otros workers por esa pérdida de propiedad. Los demás errores de liquidación siguen deteniendo el consumidor.

## Limitaciones

- La cancelación es cooperativa: un handler que ignore el contexto puede seguir realizando efectos externos. El fencing protege las escrituras transaccionales, no una llamada externa ya iniciada.
- `Send` no informa de si un mensaje deduplicado se descartó.
- Un mensaje en curso también cuenta para la deduplicación: un `Send` con su clave mientras se procesa se descarta, aunque el handler ya haya leído el estado.
- No hay orden garantizado entre mensajes más allá de `available_at, id`, y un reintento o un lease caducado lo altera.
- La conexión de `LISTEN` no pasa por los hooks del pool (`BeforeConnect`, `AfterConnect`); si dependen de ellos las credenciales, el transporte cae a sondeo.

## Uso desde delusic

Pasos para sustituir `internal/platform/jobs` de delusic (colas `resource_jobs` y `resource_cleanup_jobs`) por este transporte:

1. **Migración.** Nuevas tablas con `postgres.Schema("resource_verifications")` y `postgres.Schema("resource_cleanups")` (nombres de ejemplo), más `org_id`/`resource_id` y una clave foránea con `ON DELETE CASCADE` hacia `resources` solo en la de verificación si se quiere conservar el "el job desaparece con el recurso" (columnas con valor por defecto o nulas, ver "La tabla"). Copiar los jobs pendientes en la misma migración, como mensajes con `attempt = 0`, o drenar las colas viejas antes de desplegar.
2. **Mensajes** en `internal/resources`, con alias estables:
   - `VerifyResource{OrgID, ResourceID}`, `Name() = "resources.verify"`, `DedupKey() = "verify:" + ResourceID`: un único job de verificación por recurso, como el `UNIQUE (subject_id)` actual.
   - `CleanupResource{OrgID, ResourceID, StorageKey}`, `Name() = "resources.cleanup"`, `DedupKey() = "cleanup:" + ResourceID`. La clave de almacenamiento viaja en el payload en vez de en un `ObjectKeyStamp`.
3. **Router y bus compartidos** en `cmd/api` y `cmd/worker`: un transporte por tabla con `postgres.WithDB(executor)` (el `database.DB` del diseño del ejecutor), `router.Route[VerifyResource](verifications)` y `router.Route[CleanupResource](cleanups)`, y un bus `qumsg.New(qumsg.WithRouter(router))`. Para el trazado, un `Propagator` que inyecte `traceparent` en los metadatos sustituye al `TraceStamp`.
4. **Envío dentro de las transacciones del servicio.** En `Service.Complete`, dentro de `WithinTransaction`, tras `StartProcessing`: `bus.Dispatch(ctx, VerifyResource{…})` en lugar de `tx.Jobs.Enqueue`. En `Service.Delete`, tras borrar: `bus.Dispatch(postgres.ContextWithDelay(ctx, delay), CleanupResource{…})`, con `delay` = TTL de la URL prefirmada si el recurso seguía subiendo y 0 si no (lo que hoy calcula `cleanupAt`). El ejecutor resuelve la transacción del `ctx`, así que mensaje y escritura se confirman o se revierten juntos.
5. **Workers.** Ambos consumen con `qumsg.WithUnlimitedAttempts()` y `qumsg.WithCancelOnShutdown()`. El verificador consulta `qumsg.Attempt(ctx)` y registra el fallo terminal junto con `Complete` en su transacción; si esa escritura falla, el mensaje se conserva para reintentar. La limpieza reintenta sin límite. Opciones de las colas actuales: `WithLease(2*time.Minute)` y `WithBackoff(jobs.DefaultBackoff)` (10 s doblando hasta 10 min).
6. **Idempotencia.** `FinishVerification` y `FailVerification` ya solo cambian recursos en `processing`, y el `Cleaner` ya tolera repetir el borrado; con eso basta para la entrega al menos una vez. Si se quiere conservar la atomicidad actual de "escritura final + fin del job", el bus del worker añade `transport.Propagator()` y esos métodos llaman a `transport.Complete(ctx, executor)` dentro de su `WithinTransaction` en lugar de `queue.Complete`.
7. **Lo que debe conservar la aplicación:** el `SubjectLocker` (lock consultivo por recurso alrededor del handler para excluir un borrado concurrente) y las métricas del procesado. El transporte proporciona cancelación al perder el lease y liberación sin contar intento al apagar; no sustituye la exclusión por recurso.

## Desarrollo

Los tests unitarios no necesitan PostgreSQL. Los de integración y los ejemplos llevan el build tag `integration` y crean una tabla aleatoria por test:

```sh
docker compose up -d --wait postgres
go test -race -tags integration ./postgres/...
```

`POSTGRES_URL` cambia la conexión (por defecto `postgres://qumsg:qumsg@localhost:5432/qumsg?sslmode=disable`) y `POSTGRES_PORT` el puerto publicado por `compose.yaml`.

## Pendiente

Implementación terminada el 2026-10-12 e incluida en `v0.7.0`.

Hecho:

- Documentación: este README, `AGENTS.md`, `CHANGELOG.md` y el README raíz.
- `postgres.go`: `New`, `Schema`, opciones, `Send` (con `WithDB`, deduplicación, `ContextWithDelay` y `NOTIFY`), `Receive` (reclamación con `SKIP LOCKED`, leases caducados, `LISTEN/NOTIFY` y sondeo), latido, `Ack`/`Nack` cercados, `Accept`, `UndecodableMessage`, `Propagator` y `Complete`.
- Tests unitarios (`postgres_test.go`) con un `DB` falso: validaciones, SQL y argumentos de `Send`, deduplicación y retraso, `Close`, `Propagator` y `Complete`.
- Tests de integración contra PostgreSQL real: transacciones, deduplicación, retraso y reintentos, reclamación concurrente, `SKIP LOCKED`, recuperación y fencing, renovación de leases, `Complete` durante el latido, notificaciones, cancelación, cierre y mensajes inválidos hasta el `FailureHandler`.
- Ejemplos ejecutables del productor/worker, envío transaccional, `Complete`, deduplicación y envío diferido.
- Servicio `postgres` en `compose.yaml`, objetivos del `Makefile` para los tres módulos y CI; release prepara las etiquetas de Valkey y PostgreSQL sobre el mismo commit.
- Identificadores SQL entrecomillados: también funcionan nombres de tabla reservados como `select`, cubierto por una prueba de regresión contra PostgreSQL.

Verificación: `make check` pasa con PostgreSQL y Valkey aislados (formato, vet, unitarios e integración con `-race`), incluidos los cinco ejemplos ejecutables. El workflow de release verifica cada transporte contra el núcleo publicado, actualiza su dependencia a `v0.7.0` y crea las etiquetas `valkey/v0.7.0` y `postgres/v0.7.0`.

Cambios publicados en `v0.8.0`:

- `DeliveryContext`, `ErrDeliveryLost`, `ReleasableDelivery`, `WithCancelOnShutdown`, `WithUnlimitedAttempts` y `Attempt(ctx)` en el núcleo.
- Cancelación del handler ante renovación incierta, `Release` sin contar intento, token único por reclamación y `Own` transaccional en PostgreSQL.

Sigue pendiente informar del resultado de deduplicación: `Transport.Send` solo devuelve `error`.
