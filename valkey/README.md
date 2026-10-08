# qumsg/valkey

Transporte de [qumsg](../README.md) sobre [Valkey Streams](https://valkey.io/topics/streams-intro/). Permite enviar mensajes desde un proceso y consumirlos en otro, con varios workers compartiendo el trabajo, reintentos y recuperación de mensajes de consumidores caídos.

Es un módulo aparte para que el núcleo de `qumsg` no dependa del cliente de Valkey:

```sh
go get github.com/romycode/qumsg/valkey
```

Usa el cliente oficial [`valkey-go`](https://github.com/valkey-io/valkey-go). Como los dos paquetes se llaman `valkey`, los ejemplos importan el cliente como `vk`. Requiere Go 1.27+ y Valkey 7+ (o Redis 6.2+).

## Uso

Los mensajes que viajan por Valkey deben implementar `qumsg.NamedMessage`. El alias es su identificador en el stream, así que puedes renombrar o mover el tipo Go sin romper los mensajes ya encolados:

```go
type SendWelcomeEmail struct {
    Email string
}

func (SendWelcomeEmail) Name() string {
    return "emails.welcome"
}
```

El proceso productor enruta el mensaje al stream:

```go
client, err := vk.NewClient(vk.ClientOption{InitAddress: []string{"localhost:6379"}})
if err != nil {
    return err
}
defer client.Close()

producer := qumsg.New()
if err := producer.Route[SendWelcomeEmail](valkey.New(client, "emails")); err != nil {
    return err
}

err = producer.Dispatch(ctx, SendWelcomeEmail{Email: "albert@example.com"})
```

El proceso worker declara qué tipos sabe decodificar y consume el stream:

```go
transport := valkey.New(client, "emails")
if err := transport.Register[SendWelcomeEmail](); err != nil {
    return err
}

worker := qumsg.New()
if err := worker.Register(sendWelcomeEmail); err != nil {
    return err
}

return worker.Consume(ctx, transport, qumsg.WithWorkers(4))
```

`Register[T]` es un método genérico con las mismas reglas que `qumsg.Bus.Register`: el alias no puede estar vacío ni repetirse entre tipos. El productor no necesita registrar los tipos; solo los usa para obtener su alias.

Para una familia de mensajes, comparte una lista de [`qumsg.Type`](../README.md#familias-de-mensajes) entre el productor y el worker:

```go
var EmailMessages = []qumsg.Type{
    qumsg.TypeOf[SendWelcomeEmail](),
    qumsg.TypeOf[SendPasswordReset](),
}

_ = producer.RouteTypes(valkey.New(client, "emails"), EmailMessages...) // productor
_ = transport.RegisterTypes(EmailMessages...)                          // worker
```

`RegisterTypes` registra todos los tipos de la lista o ninguno: si alguno no es válido o su alias ya está ocupado, devuelve el error sin registrar los demás.

## Cómo funciona

Cada mensaje es una entrada del stream con tres campos: `name` (el alias), `payload` (el mensaje codificado, JSON por defecto) y `attempt` (el número de intento).

| Operación | Comandos |
| --- | --- |
| `Send` | `XADD` |
| `Receive` | `XAUTOCLAIM` para recuperar mensajes abandonados y, si no hay, `XREADGROUP` |
| `Ack` | `XACK` |
| `Nack` | `XADD` del mismo mensaje con `attempt + 1` y `XACK` del original |

- **Grupo de consumidores.** Todos los transportes con el mismo stream y grupo se reparten los mensajes: cada uno lo procesa un solo consumidor. El grupo se crea al primer `Receive` y lee el stream desde el principio, así que no se pierden los mensajes enviados antes de arrancar el primer worker.
- **Reintentos.** `Nack` vuelve a añadir el mensaje al final del stream con el intento incrementado, y se entrega de nuevo en cuanto un worker queda libre. No hay retraso entre reintentos.
- **Consumidores caídos.** Un mensaje recibido y nunca confirmado queda pendiente. Cuando lleva más tiempo que `WithClaimIdle` sin confirmarse, otro consumidor lo reclama con `XAUTOCLAIM` y cuenta esa entrega como un intento más. `WithClaimIdle` debe ser mayor que el handler más lento; si no, un mensaje en curso se procesará dos veces.
- **Entrega al menos una vez.** Si un proceso cae después del handler y antes de `XACK`, o entre los dos comandos de `Nack`, el mensaje se procesará otra vez. Los handlers deben tolerar duplicados.
- **Mensajes que no se pueden decodificar.** Si el alias no está registrado o el payload no se puede decodificar, se entrega un `valkey.UndecodableMessage` con el nombre, el payload y el error. Normalmente no tiene handler, así que falla con `qumsg.ErrNoHandler`, agota sus intentos y llega al `FailureHandler` de `Consume`. Un mensaje corrupto no detiene a los workers.
- **Contexto.** `valkey-go` solo interrumpe un comando bloqueante al vencer un deadline, no al cancelarse el contexto. Por eso `Receive` bloquea en tramos de `WithBlock` y revisa entre uno y otro si el contexto se ha cancelado o el transporte se ha cerrado.

## Opciones

| Opción | Por defecto | Qué controla |
| --- | --- | --- |
| `WithGroup(name)` | `qumsg` | Grupo de consumidores. |
| `WithConsumer(name)` | hostname + sufijo aleatorio | Nombre del consumidor; único por proceso. |
| `WithBlock(d)` | `1s` | Espera máxima de cada `XREADGROUP`, y por tanto cuánto tarda `Receive` en notar una cancelación o `Close`. |
| `WithClaimIdle(d)` | `1m` | Tiempo sin confirmar tras el que otro consumidor reclama un mensaje. |
| `WithMaxLen(n)` | sin límite | Recorta el stream a unas `n` entradas en cada `XADD`. Puede borrar mensajes sin consumir si la cola crece más. |
| `WithCodec(c)` | JSON | Codificación del payload. |

## Errores

Además de los errores de Valkey, que se devuelven envueltos con la operación:

| Error | Cuándo |
| --- | --- |
| `valkey.ErrUnnamedMessage` | `Send` o `Register` con un tipo que no implementa `qumsg.NamedMessage`. |
| `qumsg.ErrEmptyName`, `qumsg.ErrNamePanic`, `qumsg.ErrDuplicateName`, `qumsg.ErrInterfaceType` | Las mismas validaciones del alias que hace `qumsg.Bus.Register`. |
| `qumsg.ErrNilMessage` | `Send(ctx, nil)`. |
| `qumsg.ErrTransportClosed` | `Send` o `Receive` después de `Close`. `Close` no borra los mensajes del stream ni cierra el cliente. |

## Desarrollo

Los tests unitarios no necesitan Valkey. Los de integración y el ejemplo llevan el build tag `integration`:

```sh
docker compose up -d --wait valkey
go test -race -tags integration ./valkey/...
```

`VALKEY_ADDR` cambia la dirección (por defecto `localhost:6379`).
