# Gaps pendientes

Problemas detectados en la revisión de código del 2026-11-13 (sobre `v0.8.4`), pendientes de resolver. Salen de leer el código: ninguno está confirmado con un test que lo reproduzca, así que el primer paso de cada uno es escribir ese test.

Al resolver uno, bórralo de aquí y anótalo en `[Sin publicar]` del `CHANGELOG.md`.

## Bugs

### 1. Un handler que termina bien se reentrega si el lease se dio por perdido

- **Dónde:** `transport.go`, `settle` (el `case` de `ErrDeliveryLost`).
- **Qué pasa:** el caso `ErrDeliveryLost` se evalúa antes que `err == nil` y hace `Nack`. En `postgres`, cualquier fallo transitorio de una renovación cancela el contexto aunque la fila siga siendo de la entrega. Si el handler acaba con éxito, el `Nack` cercado funciona y el mensaje se procesa otra vez, con un intento contado.
- **Arreglo propuesto:** con `err == nil`, intentar `Ack`. Está cercado: si el lease se perdió de verdad devuelve `ErrLeaseLost` sin cambiar nada; si no, evita el duplicado.
- **Prioridad:** alta.

### 2. `Send` de `postgres` puede devolver error con el mensaje ya encolado

- **Dónde:** `postgres/postgres.go`, `Send` (`INSERT` y `pg_notify` en dos `Exec`).
- **Qué pasa:** con el pool (sin transacción), si el `pg_notify` falla el mensaje ya está confirmado pero `Dispatch` devuelve error, y quien reintente lo duplica (salvo que sea `Deduplicated`).
- **Arreglo propuesto:** una sola sentencia: `WITH ins AS (INSERT … RETURNING 1) SELECT pg_notify(…) FROM ins`.
- **Prioridad:** alta.

### 3. Un `NOTIFY` fallido tras `Nack` o `Release` para todo `Consume`

- **Dónde:** `postgres/postgres.go`, `Nack` y `Release` (el `pg_notify` posterior a `fenced`).
- **Qué pasa:** la liquidación ya se aplicó y el sondeo cubre la notificación perdida, pero el error no es `ErrDeliveryLost`, así que `Consume` cancela a todos los workers y termina.
- **Arreglo propuesto:** meter el `pg_notify` en la misma sentencia que la liquidación, o ignorar ese error.
- **Prioridad:** alta.

### 4. Reclamación huérfana al cancelar `Receive`

- **Dónde:** `postgres/postgres.go`, `claim`.
- **Qué pasa:** si el `ctx` se cancela con el `UPDATE` ya ejecutado en el servidor, la fila queda bloqueada con un token que nadie tiene. El mensaje es invisible hasta que caduca el lease (1 min por defecto) y pierde un intento. Pasa sobre todo en apagados: con `WithMaxAttempts(3)`, varios reinicios seguidos pueden mandar un mensaje sano al `FailureHandler`.
- **Arreglo propuesto:** por decidir. Opciones: ejecutar la reclamación con un contexto no cancelable y acotado, y hacer `Release` si el `ctx` ya estaba cancelado al volver; o documentarlo como límite conocido.
- **Prioridad:** media.

### 5. `Complete` seguido de más trabajo cancela el handler

- **Dónde:** `postgres/postgres.go`, `startHeartbeat` (renovación que afecta a 0 filas).
- **Qué pasa:** tras confirmar `Complete` la fila no existe, la siguiente renovación afecta a 0 filas y el contexto del handler se cancela con `ErrLeaseLost`. No hay duplicado, pero el trabajo posterior al commit dentro del handler se corta.
- **Arreglo propuesto:** distinguir "fila borrada" de "fila con otro dueño" en la renovación y, si está borrada, parar el latido sin cancelar. Como mínimo, documentarlo en `postgres/README.md`.
- **Prioridad:** media.

### 6. `valkey`: `XAUTOCLAIM` siempre empieza en `0-0` con `COUNT 1`

- **Dónde:** `valkey/valkey.go`, `claim`.
- **Qué pasa:** cada llamada examina unas 10 entradas pendientes y se ignora el cursor devuelto. Si las 10 primeras están pendientes pero recién reclamadas, un mensaje abandonado con ID mayor no se recupera hasta que esas terminen.
- **Arreglo propuesto:** guardar el cursor que devuelve `XAUTOCLAIM` y continuar desde él, volviendo a `0-0` al llegar al final.
- **Prioridad:** baja.

### 7. `valkey` no valida sus opciones

- **Dónde:** `valkey/valkey.go`, `WithGroup`, `WithConsumer`, `WithCodec`.
- **Qué pasa:** `WithCodec(nil)` provoca un pánico por puntero nulo en el primer `Send`; `WithGroup("")` y `WithConsumer("")` fallan tarde, en el servidor. `postgres` sí valida las equivalentes.
- **Arreglo propuesto:** pánico al configurar, como en `postgres`, y añadirlo a la lista de pánicos del README.
- **Prioridad:** baja.

### 8. `memory`: `Send` que compite con `Close` puede perder el mensaje devolviendo `nil`

- **Dónde:** `memory/memory.go`, `enqueue`.
- **Qué pasa:** si `Close` ocurre entre la comprobación de `closed` y el `select`, el mensaje puede entrar en la cola cuando los consumidores ya han salido.
- **Arreglo propuesto:** encolar bajo `mu`, o documentarlo: el transporte ya es con pérdida.
- **Prioridad:** baja.

## Seguridad y endurecimiento

### 9. Actions fijadas por etiqueta, no por SHA

- **Dónde:** `.github/workflows/*.yml` (`actions/checkout@v5`, `actions/setup-go@v6`).
- **Arreglo propuesto:** fijar por SHA de commit con la versión en un comentario.

### 10. Token con escritura disponible mientras corren los tests del release

- **Dónde:** `.github/workflows/release.yml`.
- **Qué pasa:** el token con `contents: write` queda en la configuración global de git mientras se ejecuta `go test`. Riesgo bajo: es código propio en `main`.
- **Arreglo propuesto:** separar en dos jobs: uno de solo lectura que prueba, y otro con escritura que hace commit y etiqueta.

### 11. Servicios de `compose.yaml` publicados en todas las interfaces

- **Dónde:** `compose.yaml` (Valkey sin contraseña, Postgres con `qumsg/qumsg`).
- **Arreglo propuesto:** publicar en `127.0.0.1` (`127.0.0.1:${VALKEY_PORT:-6379}:6379`, igual para Postgres).

## Documentación

### 12. Tamaño del pool frente al número de workers

- **Dónde:** `postgres/README.md`.
- **Qué falta:** el latido usa el mismo pool que los handlers. Si `MaxConns` es menor o igual que el número de workers y cada handler retiene una conexión, las renovaciones se quedan sin conexión y los leases se pierden. Documentar un mínimo recomendado.

### 13. Los metadatos de la cola no están autenticados

- **Dónde:** `README.md`, sección de propagadores.
- **Qué falta:** quien pueda escribir en el stream o en la tabla puede inyectar metadatos que `ContextValue` mete en el `ctx`. Avisar de que no sirven para autorizar (usuario, tenant) sin validarlos.
