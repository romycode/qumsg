# qumsg

Bus de mensajes síncrono para Go, inspirado en Symfony Messenger. Permite despachar structs a handlers tipados y envolver su ejecución con middleware.

La aplicación crea un mensaje, lo entrega al bus y espera a que terminen sus handlers. El bus centraliza la selección de handlers y las operaciones transversales, mientras que cada handler contiene la lógica de la aplicación.

> **Estado:** `v0.1.0`, primera versión. Requiere Go 1.27+. Mientras la versión sea `0.x` la API puede cambiar; consulta el [CHANGELOG](CHANGELOG.md).

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

## Qué incluye la primera versión

- Ejecución síncrona en la goroutine que llama a `Dispatch`.
- Mensajes sin métodos obligatorios.
- Registro explícito de handlers tipados.
- Varios handlers para un mismo tipo de mensaje.
- Ejecución en orden de registro y parada en el primer error.
- Middleware alrededor del dispatch completo.
- Identificación por tipo Go y alias opcional mediante `Name()`.
- Propagación de `context.Context`.

La primera versión no incluye colas, workers, transportes, persistencia, serialización, reintentos ni ejecución paralela de handlers. Tampoco incorpora un envelope o un sistema de stamps.

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
3. Si cualquiera devuelve un error, se interrumpe la ejecución y se devuelve ese error.

El bus no revierte los efectos de los handlers que ya han terminado. Repetir manualmente el dispatch vuelve a ejecutar la secuencia desde el principio.

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
bus := qumsg.New(Logging(slog.Default()))
```

El middleware puede ejecutar código antes y después, derivar un contexto para la siguiente operación o interrumpir el flujo devolviendo un error sin llamar a `next`.

### Orden de ejecución

Para `qumsg.New(first, second)`, el orden es:

1. Entrada de `first`.
2. Entrada de `second`.
3. Ejecución de los handlers, en orden de registro.
4. Salida de `second`.
5. Salida de `first`.

El primer middleware configurado es el más externo. La cadena se ejecuta una vez por dispatch, no una vez por handler.

Un middleware habitual llama a `next` exactamente una vez. Llamarlo varias veces repite la ejecución posterior, incluidos sus efectos.

## API

| Operación | Responsabilidad |
| --- | --- |
| `New(middlewares ...Middleware) *Bus` | Crea un bus con su cadena de middleware. |
| `(*Bus).Register[T any](handler func(context.Context, T) error) error` | Registra un handler para el tipo exacto `T` y valida su alias opcional. |
| `(*Bus).Dispatch(ctx context.Context, message any) error` | Ejecuta el middleware y los handlers correspondientes. |
| `NamedMessage.Name() string` | Proporciona un alias opcional, constante y único por tipo dentro del bus. |

`Register` es un método genérico (Go 1.27+): `T` se infiere del handler, así que basta con `bus.Register(welcomeUser)`. El bus guarda los handlers de cada tipo ya tipados, de modo que un dispatch convierte el mensaje a `T` una sola vez y no por cada handler. `Dispatch` recibe `any` a propósito: resuelve por el tipo dinámico del mensaje y comparte la firma con el middleware.

## Errores y contexto

Un dispatch sin un handler registrado para su tipo devuelve un error, salvo que un middleware interrumpa antes el flujo.

El error de un handler detiene los siguientes handlers y vuelve a través del middleware hasta el llamador. Si la implementación añade contexto al error, debe conservarlo para permitir su inspección mediante `errors.Is` y `errors.As`.

El contexto se propaga desde `Dispatch` hasta los handlers, pasando por los middleware. Los handlers deben trasladarlo a las operaciones que realizan, como consultas o peticiones HTTP.

La cancelación es cooperativa: no puede detener por la fuerza un handler que ignore el contexto. Antes de ejecutar cada handler, `Dispatch` comprueba `ctx.Err()`; si el contexto está cancelado, no ejecuta los handlers restantes y devuelve ese error.

Los errores de un handler se devuelven sin envolver. Los errores propios del bus son variables exportadas, envueltas con el tipo afectado y comparables con `errors.Is`:

| Error | Cuándo |
| --- | --- |
| `ErrNoHandler` | `Dispatch` con un tipo sin handlers. |
| `ErrNilMessage` | `Dispatch(ctx, nil)`. |
| `ErrNilHandler` | `Register` con un handler `nil`. |
| `ErrInterfaceType` | `Register` con `T` interfaz: nunca coincidiría con el tipo concreto de un mensaje. |
| `ErrEmptyName` | `Name()` devuelve una cadena vacía. |
| `ErrNamePanic` | `Name()` entra en pánico sobre el valor cero, normalmente porque depende de campos. |
| `ErrDuplicateName` | Otro tipo ya usa ese alias en el bus. |

`New` entra en pánico si recibe un middleware `nil`, porque es un error de programación detectado al configurar.

El bus no recupera panics automáticamente. Una política de recuperación, si la aplicación la necesita, puede implementarse como middleware.

## Configuración y concurrencia

Configura todos los handlers antes de empezar a despachar mensajes. Registrar durante un dispatch no provoca carreras de datos, pero no está soportado: un dispatch en curso puede ver o no el nuevo handler.

Una vez configurado, el bus permite utilizar el bus desde varias goroutines. Cada dispatch ejecuta sus propios handlers secuencialmente, pero dos dispatches pueden coincidir y llamar al mismo handler a la vez.

Los handlers y los middleware son responsables de sincronizar el estado compartido al que acceden. El bus no crea goroutines ni copia en profundidad los mensajes: punteros, mapas o slices pueden compartir datos con el llamador.

## Desarrollo

```sh
go test -race ./...
```

Los ejemplos de este README están en `example_test.go` como funciones `Example` y se verifican con `go test`. Los tests unitarios de `bus_test.go` cubren la resolución por tipo exacto, el orden de handlers y middleware, la parada en el primer error, los alias y sus colisiones, la propagación del contexto y la cancelación, la conservación de errores y los dispatches concurrentes.
