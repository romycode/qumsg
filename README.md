# qumsg

Bus de mensajes para Go, inspirado en Symfony Messenger. Permite despachar structs a handlers tipados, envolver su ejecución con middleware y, opcionalmente, enviarlos a un transporte para procesarlos de forma asíncrona.

La aplicación crea un mensaje, lo entrega al bus y espera a que terminen sus handlers. El bus centraliza la selección de handlers y las operaciones transversales, mientras que cada handler contiene la lógica de la aplicación.

> **Estado:** `v0.4.0`. Requiere Go 1.27+. Mientras la versión sea `0.x` la API puede cambiar; consulta el [CHANGELOG](CHANGELOG.md).

```sh
go get github.com/romycode/qumsg
```

## Primer vistazo

Define un mensaje y una función que lo procese:

```go
type UserCreated struct {
    ID    string
    Email string
}

func welcomeUser(ctx context.Context, msg UserCreated) error {
    fmt.Printf("Welcome, %s\n", msg.Email)
    return nil
}
```

Crea el bus, registra el handler y despacha el mensaje:

```go
func run(ctx context.Context) error {
    bus := qumsg.New()

    if err := bus.Register(welcomeUser); err != nil {
        return err
    }

    return bus.Dispatch(ctx, UserCreated{
        ID:    "user-123",
        Email: "albert@example.com",
    })
}
```

`Register` deduce el tipo del mensaje a partir de la firma del handler. `Dispatch` utiliza el tipo concreto recibido para encontrar los handlers registrados.

Los ejemplos utilizan paquetes estándar como `context`, `fmt`, `log/slog` y `time`; los imports se omiten para centrar la lectura en el bus.

## Qué incluye

- Ejecución síncrona en la goroutine que llama a `Dispatch`.
- Mensajes sin métodos obligatorios.
- Registro explícito de handlers tipados.
- Varios handlers para un mismo tipo de mensaje.
- Ejecución en orden de registro y parada en el primer error, o ejecución de todos los handlers con `ContinueOnError`.
- Opciones del bus para decidir si un mensaje sin handlers es un error (`AllowNoHandler`). El bus no distingue entre comandos y eventos: cada aplicación configura sus buses.
- Listas de tipos (`qumsg.Type`) para registrar o enrutar una familia de mensajes de una vez.
- Middleware alrededor del dispatch completo.
- Identificación por tipo Go y alias opcional mediante `Name()`.
- Propagación de `context.Context`.
- Ejecución asíncrona opcional: rutas hacia un transporte, consumo con workers, reintentos y apagado ordenado.
- Un transporte en memoria (`qumsg/memory`).
- Un transporte sobre Valkey Streams en el módulo [`qumsg/valkey`](valkey/README.md), con persistencia, varios procesos y serialización JSON. Tampoco incorpora un envelope o un sistema de stamps: los handlers de un mismo dispatch se siguen ejecutando secuencialmente.

## Mensajes e identidad

Un mensaje puede ser un struct normal:

```go
type ProductCreated struct {
    ID string
}
```

No necesita implementar ninguna interfaz. Su identidad principal es su tipo Go; no una cadena generada a partir del nombre del struct.

La resolución es por coincidencia exacta de tipo. `UserCreated` y `*UserCreated` son tipos diferentes: registra y despacha usando la misma forma. No hay conversión automática entre valores y punteros ni búsqueda de handlers por interfaces implementadas.

### Nombre opcional

Un mensaje puede proporcionar un identificador alternativo:

```go
type NamedMessage interface {
    Name() string
}
```

Por ejemplo:

```go
type UserCreated struct {
    ID    string
    Email string
}

func (UserCreated) Name() string {
    return "users.created"
}
```

El alias queda asociado al tipo durante el registro y cumple estas reglas:

- Es único dentro de cada instancia del bus.
- Es constante para ese tipo e independiente del contenido del mensaje.
- No puede estar vacío.
- Registrar más handlers para el mismo tipo y alias está permitido.
- Registrar otro tipo con el mismo alias devuelve un error y no añade el handler.
- Un tipo que no implementa `NamedMessage` no necesita un alias.

Define `Name()` sin depender de campos inicializados, como en el ejemplo: `Register` lo llama sobre el valor cero del tipo (o un puntero a él). Si `Name()` entra en pánico, `Register` lo recupera y devuelve `ErrNamePanic` sin añadir el handler.

El alias puede utilizarse en logs y otras herramientas de identificación. `Dispatch` sigue resolviendo por tipo: esta versión no expone un dispatch por nombre ni convierte mensajes entre tipos.

Como `T` y `*T` son tipos diferentes, si ambos ofrecen el mismo alias no se pueden registrar los dos en el mismo bus.

## Handlers

Un handler tiene esta forma:

```go
func(ctx context.Context, msg T) error
```

Puede ser una función, una closure o un método enlazado a una instancia:

```go
type WelcomeService struct {
    // Dependencias de la aplicación.
}

func (s *WelcomeService) Handle(
    ctx context.Context,
    msg UserCreated,
) error {
    // Ejecutar la lógica de la aplicación.
    return nil
}

// Durante la configuración:
service := &WelcomeService{}
err := bus.Register(service.Handle)
```

El handler recibe el mensaje tipado y devuelve `nil` si termina correctamente, o un error si falla. En esta versión no devuelve un resultado de negocio a través del bus.

### Varios handlers

```go
if err := bus.Register(welcomeUser); err != nil {
    return err
}

if err := bus.Register(auditUserCreated); err != nil {
    return err
}
```

Al despachar un `UserCreated`:

1. Se ejecuta `welcomeUser`.
2. Si devuelve `nil`, se ejecuta `auditUserCreated`.
3. Si cualquiera devuelve un error, se interrumpe la ejecución y se devuelve ese error (salvo con [`ContinueOnError`](#opciones-del-bus)).

El bus no revierte los efectos de los handlers que ya han terminado. Repetir manualmente el dispatch vuelve a ejecutar la secuencia desde el principio.

## Opciones del bus

El bus no sabe si un mensaje es un comando o un evento. Por defecto, un mensaje sin handlers devuelve `ErrNoHandler` y el primer error detiene al resto, lo que encaja con mensajes que alguien tiene que procesar. Dos opciones cambian ese comportamiento:

| Opción | Efecto |
| --- | --- |
| `AllowNoHandler()` | Un mensaje sin handlers no es un error: `Dispatch` devuelve `nil`. |
| `ContinueOnError()` | Se ejecutan todos los handlers aunque alguno falle, y sus errores se devuelven unidos con `errors.Join`; si solo falla uno, se devuelve sin envolver. |
| `WithMiddleware(mws...)` | Añade middleware; ver [Middleware](#middleware). |

Una aplicación que necesite ambos comportamientos crea un bus para cada uno:

```go
type OrderShipped struct{ ID string }

strict := qumsg.New()
lenient := qumsg.New(qumsg.AllowNoHandler(), qumsg.ContinueOnError())

for _, bus := range []*qumsg.Bus{strict, lenient} {
    _ = bus.Register(func(ctx context.Context, e OrderShipped) error {
        fmt.Println("email for", e.ID)
        return errors.New("smtp down")
    })
    _ = bus.Register(func(ctx context.Context, e OrderShipped) error {
        fmt.Println("stock for", e.ID)
        return nil
    })
}

strict.Dispatch(ctx, OrderShipped{ID: "o-1"})  // email; devuelve "smtp down" sin ejecutar stock
strict.Dispatch(ctx, UserCreated{ID: "u-1"})   // ErrNoHandler
lenient.Dispatch(ctx, OrderShipped{ID: "o-2"}) // email y stock; devuelve "smtp down"
lenient.Dispatch(ctx, UserCreated{ID: "u-2"})  // nil
```

Con cualquiera de las opciones, el contexto se comprueba antes de cada handler y el middleware envuelve el dispatch completo.

## Familias de mensajes

`Register[T]` y `Route[T]` son métodos genéricos, y un método genérico no puede formar parte de una interfaz ni aplicarse a una lista de tipos. Para manejar una familia de mensajes (los eventos de un paquete, por ejemplo) sin repetir una llamada por tipo en cada sitio, `qumsg.TypeOf[T]()` captura el tipo en un valor y la lista se escribe una sola vez:

```go
type InvoicePaid struct{ ID string }
type InvoiceVoided struct{ ID string }

var InvoiceEvents = []qumsg.Type{
    qumsg.TypeOf[InvoicePaid](),
    qumsg.TypeOf[InvoiceVoided](),
}

// Un handler para todos los tipos, por ejemplo un registro de auditoría.
_ = bus.RegisterFor(func(ctx context.Context, msg any) error {
    fmt.Printf("audit %T\n", msg)
    return nil
}, InvoiceEvents...)

// O enviarlos todos a un transporte.
_ = producer.RouteTypes(transport, InvoiceEvents...)
```

`RegisterFor` equivale a llamar a `Register` por cada tipo con un handler que recibe el mensaje como `any`, así que se ejecuta en orden de registro junto a los handlers tipados y respeta los alias. Valida todos los tipos antes de registrar ninguno. `TypeOf` nunca falla: si `T` no es un tipo de mensaje válido, `Type.Err` lo indica y el registro devuelve ese error. Los transportes usan `Type.Decode` para decodificar mensajes sin ser genéricos, como hace `valkey.Transport.RegisterTypes`.

## Middleware

Un middleware envuelve la ejecución completa de un mensaje:

```go
type HandleFunc func(ctx context.Context, message any) error

type Middleware func(next HandleFunc) HandleFunc
```

Los handlers de la aplicación permanecen tipados; el middleware recibe `any` porque puede observar distintos tipos de mensajes.

Ejemplo de logging:

```go
func Logging(logger *slog.Logger) qumsg.Middleware {
    return func(next qumsg.HandleFunc) qumsg.HandleFunc {
        return func(ctx context.Context, msg any) error {
            started := time.Now()
            err := next(ctx, msg)

            attrs := []any{
                "message_type", fmt.Sprintf("%T", msg),
                "duration", time.Since(started),
            }

            if named, ok := msg.(qumsg.NamedMessage); ok {
                attrs = append(attrs, "message_name", named.Name())
            }

            if err != nil {
                attrs = append(attrs, "error", err)
                logger.ErrorContext(ctx, "Message dispatch failed", attrs...)
            } else {
                logger.InfoContext(ctx, "Message dispatched", attrs...)
            }

            return err
        }
    }
}
```

Se configura al crear el bus:

```go
bus := qumsg.New(qumsg.WithMiddleware(Logging(slog.Default())))
```

El middleware puede ejecutar código antes y después, derivar un contexto para la siguiente operación o interrumpir el flujo devolviendo un error sin llamar a `next`.

### Orden de ejecución

Para `qumsg.New(qumsg.WithMiddleware(first, second))`, el orden es:

1. Entrada de `first`.
2. Entrada de `second`.
3. Ejecución de los handlers, en orden de registro.
4. Salida de `second`.
5. Salida de `first`.

El primer middleware configurado es el más externo; varias opciones `WithMiddleware` añaden el suyo en orden. La cadena se ejecuta una vez por dispatch, no una vez por handler.

Un middleware habitual llama a `next` exactamente una vez. Llamarlo varias veces repite la ejecución posterior, incluidos sus efectos.

## Ejecución asíncrona

Un transporte desacopla quien envía un mensaje de quien lo procesa. El bus que produce el mensaje lo enruta al transporte, y un bus de worker lo recibe y lo despacha a los handlers reales:

```go
transport := memory.New()

producer := qumsg.New()
if err := producer.Route[SendWelcomeEmail](transport); err != nil {
    return err
}

worker := qumsg.New(qumsg.WithMiddleware(Logging(slog.Default())))
if err := worker.Register(sendWelcomeEmail); err != nil {
    return err
}

go worker.Consume(ctx, transport) // en otro proceso con un transporte externo

// Vuelve en cuanto el mensaje está en el transporte.
err := producer.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})
```

`Route[T]` es un método genérico que registra un handler que envía el mensaje al transporte en vez de procesarlo. Sigue las mismas reglas que `Register`: valida el alias y se ejecuta en orden de registro junto a los demás handlers de `T`, así que un tipo puede procesarse en local y además enviarse.

Usa buses distintos para producir y consumir. Si el bus de worker tuviera la ruta de `T` hacia el mismo transporte, cada mensaje consumido se volvería a enviar.

El contexto no viaja por el transporte: `Consume` despacha con su propio contexto, no con el de quien llamó a `Dispatch` en el productor.

### Transportes

Un transporte implementa:

```go
type Transport interface {
    Send(ctx context.Context, message any) error
    Receive(ctx context.Context) (Delivery, error)
}

type Delivery interface {
    Message() any
    Attempt() int                   // número de entrega, empezando en 1
    Ack(ctx context.Context) error  // procesado: el transporte lo olvida
    Nack(ctx context.Context) error // falló: el transporte lo vuelve a entregar
}
```

`Receive` se bloquea hasta que hay un mensaje, el contexto termina o el transporte se cierra; en este último caso devuelve `ErrTransportClosed`. Cada entrega se confirma exactamente una vez, con `Ack` o con `Nack`.

### Consumo

`Consume` recibe mensajes del transporte y los despacha en el bus hasta que el contexto termina o el transporte se cierra; en ambos casos devuelve `nil`.

```go
err := worker.Consume(ctx, transport,
    qumsg.WithWorkers(4),      // mensajes en paralelo; por defecto 1
    qumsg.WithMaxAttempts(5),  // por defecto 3
)
```

- Si el dispatch termina sin error, confirma el mensaje con `Ack`.
- Si falla y quedan intentos, lo devuelve con `Nack` para que el transporte lo entregue de nuevo.
- Si falla en el último intento, lo pasa al `FailureHandler` y lo confirma con `Ack`.

Con un solo worker y sin reintentos, los mensajes se procesan en el orden del transporte. Con varios workers o con reintentos, el orden no está garantizado.

Al cancelar el contexto, `Consume` deja de recibir y espera a los mensajes en curso. Estos terminan con un contexto que conserva los valores del original pero no se cancela con él, así que un handler no queda a medias por el apagado. Un mensaje recibido siempre se confirma.

Cualquier otro error del transporte en `Receive`, `Ack` o `Nack` detiene todos los workers y `Consume` lo devuelve.

### Reintentos y fallos

```go
err := worker.Consume(ctx, transport,
    qumsg.WithMaxAttempts(3),
    qumsg.WithFailureHandler(func(ctx context.Context, msg any, err error) {
        slog.ErrorContext(ctx, "Message failed", "message", msg, "error", err)
    }),
)
```

El `FailureHandler` recibe el mensaje y el error del último intento; puede registrarlo o enviarlo a otro transporte de mensajes fallidos. Sin `FailureHandler`, el mensaje se descarta.

La entrega es **al menos una vez**: si un worker se detiene después de ejecutar el handler y antes de confirmar el mensaje, un transporte persistente lo entregará de nuevo. Los handlers asíncronos deben tolerar mensajes duplicados.

### Transporte en memoria

`memory.New` crea un transporte respaldado por una cola acotada en el propio proceso:

```go
transport := memory.New(
    memory.WithCapacity(1000),           // por defecto 100
    memory.WithRetryDelay(time.Second),  // espera antes de reentregar; por defecto 0
)
defer transport.Close()
```

- `Send` se bloquea mientras la cola está llena, hasta que hay hueco, el contexto termina o el transporte se cierra.
- `Nack` vuelve a encolar el mensaje tras el retraso de reintento, con `Attempt` incrementado.
- `Close` deja de aceptar mensajes. Los encolados se pueden seguir recibiendo; después, `Receive` devuelve `ErrTransportClosed`. Los reintentos pendientes se descartan.

Los mensajes viven solo en memoria y se pierden si el proceso termina. Úsalo para desarrollo, tests o trabajo que se pueda perder.

### Transportes externos

| Transporte | Módulo | Documentación |
| --- | --- | --- |
| Valkey Streams | `github.com/romycode/qumsg/valkey` | [valkey/README.md](valkey/README.md) |

Cada transporte externo es un módulo Go aparte, para que el núcleo no dependa de sus clientes.

## API

| Operación | Responsabilidad |
| --- | --- |
| `New(opts ...Option) *Bus` | Crea un bus. |
| `WithMiddleware`, `AllowNoHandler`, `ContinueOnError` | Opciones de `New`. |
| `(*Bus).Register[T any](handler func(context.Context, T) error) error` | Registra un handler para el tipo exacto `T` y valida su alias opcional. |
| `(*Bus).Dispatch(ctx context.Context, message any) error` | Ejecuta el middleware y los handlers correspondientes, según las opciones del bus. |
| `TypeOf[T any]() Type` | Representa el tipo `T` como valor para usarlo en listas. |
| `(*Bus).RegisterFor(handler HandleFunc, types ...Type) error` | Registra el mismo handler para todos los tipos de la lista. |
| `(*Bus).RouteTypes(t Transport, types ...Type) error` | `Route` para todos los tipos de la lista. |
| `NamedMessage.Name() string` | Proporciona un alias opcional, constante y único por tipo dentro del bus. |
| `(*Bus).Route[T any](t Transport) error` | Registra un handler que envía los mensajes de tipo `T` al transporte. |
| `(*Bus).Consume(ctx context.Context, t Transport, opts ...ConsumeOption) error` | Recibe mensajes del transporte y los despacha en el bus. |
| `WithWorkers`, `WithMaxAttempts`, `WithFailureHandler` | Opciones de `Consume`. |
| `memory.New(opts ...memory.Option) *memory.Transport` | Crea un transporte en memoria. |

La API prefiere métodos genéricos a funciones genéricas. `Register` es un método genérico (Go 1.27+): `T` se infiere del handler, así que basta con `bus.Register(welcomeUser)`. El bus guarda los handlers de cada tipo ya tipados, de modo que un dispatch convierte el mensaje a `T` una sola vez y no por cada handler. `Dispatch` recibe `any` a propósito: resuelve por el tipo dinámico del mensaje y comparte la firma con el middleware.

## Errores y contexto

Un dispatch sin un handler registrado para su tipo devuelve un error, salvo que el bus se haya creado con `AllowNoHandler` o que un middleware interrumpa antes el flujo.

El error de un handler detiene los siguientes handlers (o, con `ContinueOnError`, se une a los de los demás) y vuelve a través del middleware hasta el llamador. Si la implementación añade contexto al error, debe conservarlo para permitir su inspección mediante `errors.Is` y `errors.As`.

El contexto se propaga desde `Dispatch` hasta los handlers, pasando por los middleware. Los handlers deben trasladarlo a las operaciones que realizan, como consultas o peticiones HTTP.

La cancelación es cooperativa: no puede detener por la fuerza un handler que ignore el contexto. Antes de ejecutar cada handler, `Dispatch` comprueba `ctx.Err()`; si el contexto está cancelado, no ejecuta los handlers restantes y devuelve ese error.

Los errores de un handler se devuelven sin envolver. Los errores propios del bus son variables exportadas, envueltas con el tipo afectado y comparables con `errors.Is`:

| Error | Cuándo |
| --- | --- |
| `ErrNoHandler` | `Dispatch` con un tipo sin handlers, salvo con `AllowNoHandler`. |
| `ErrNilMessage` | `Dispatch(ctx, nil)`. |
| `ErrNilHandler` | `Register` con un handler `nil`. |
| `ErrInterfaceType` | `Register` con `T` interfaz: nunca coincidiría con el tipo concreto de un mensaje. |
| `ErrEmptyName` | `Name()` devuelve una cadena vacía. |
| `ErrNamePanic` | `Name()` entra en pánico sobre el valor cero, normalmente porque depende de campos. |
| `ErrDuplicateName` | Otro tipo ya usa ese alias en el bus. |
| `ErrNilTransport` | `Route` o `RouteTypes` con un transporte `nil`. |
| `ErrZeroType` | Un `Type` vacío, no creado con `TypeOf`. |
| `ErrTransportClosed` | `Send` o `Receive` sobre un transporte cerrado. `Consume` lo trata como una parada normal. |

`New` entra en pánico si recibe una opción `nil`, `WithMiddleware` si recibe un middleware `nil`, y las opciones (`WithWorkers`, `WithMaxAttempts`, `memory.WithCapacity`, `memory.WithRetryDelay`) si reciben un valor fuera de rango, porque son errores de programación detectados al configurar.

El bus no recupera panics automáticamente. Una política de recuperación, si la aplicación la necesita, puede implementarse como middleware.

## Configuración y concurrencia

Configura todos los handlers antes de empezar a despachar mensajes. Registrar durante un dispatch no provoca carreras de datos, pero no está soportado: un dispatch en curso puede ver o no el nuevo handler.

Una vez configurado, el bus permite utilizar el bus desde varias goroutines. Cada dispatch ejecuta sus propios handlers secuencialmente, pero dos dispatches pueden coincidir y llamar al mismo handler a la vez.

Los handlers y los middleware son responsables de sincronizar el estado compartido al que acceden. `Dispatch` no crea goroutines; solo `Consume` lo hace, una por worker. El bus no copia en profundidad los mensajes: punteros, mapas o slices pueden compartir datos con el llamador.

## Desarrollo

El `Makefile` gestiona el entorno local; `make` sin argumentos lista los objetivos:

```sh
make test               # tests unitarios de todos los módulos
make test-integration   # levanta Valkey en Docker y ejecuta los tests de integración
make check              # formato, go vet y todos los tests: lo necesario antes de un commit
make cover              # cobertura de todos los paquetes
make bench BENCH=Parallel BENCH_CPUS=4   # benchmarks en un contenedor con recursos limitados
make down               # para los contenedores
```

El repositorio es un workspace de Go (`go.work`) con dos módulos: el núcleo en la raíz y `valkey/`. Desde la raíz, `./...` solo incluye el núcleo, así que el módulo de Valkey se indica aparte.

Los ejemplos de este README están en `example_test.go` como funciones `Example` y se verifican con `go test`. Los tests unitarios de `bus_test.go`, `transport_test.go` y `memory/memory_test.go` cubren la resolución por tipo exacto, el orden de handlers y middleware, la parada en el primer error, los alias y sus colisiones, la propagación del contexto y la cancelación, la conservación de errores, los dispatches concurrentes, el consumo con reintentos, fallos, varios workers y apagado ordenado, y el transporte en memoria. Los tests con tiempo o bloqueos usan `testing/synctest`, así que son deterministas.
