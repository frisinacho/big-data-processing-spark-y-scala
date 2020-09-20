1. Telemetría de las antenas.


## Telemetría de las antenas

Esta fuente de datos, es una fuente de datos en tiempo real en formato JSON, las distintas antenas de nuestra red enviaran mensajes con el siguiente schema:

| property  | description                     | data_type   |  example                                   |
|-----------|---------------------------------|-------------|--------------------------------------------|
| timestmap | Marca de tiempo en segundos     | LONG        | `1600528288`                               |
| id        | UUID de la antena               | STRING      | `"550e8400-e29b-41d4-a716-446655440000"`   |
| metric    | Nombre de la métrica            | STRING      | `"status"`, `"devices_count"`, `"battery"` |
| value     | Valor de la métrica             | INT         | `0`, `1`, `-1`, `90`                       |

En concreto la version 1.0.0 instaladas en el firmware de las antenas enviaran 3 tipos de mensaje:

### Mensajes de estado

El campo `metric` tomara el valor de `status`.
El campo `value` tomara los siguientes valores:
* `0`: Antena activa y funcionando correctamente.
* `-1`: Antena activa y funcionando con errores.
* `1`: Antena desactivada.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "status", "value": 0}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "status", "value": 1}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "status", "value": -1}
...
```

### Mensajes de dispositivos conectados

El campo `metric` tomara el valor de `devices_count`.
El campo `value` llevara un total de los clientes conectados en el reporte.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "devices_count", "value": 120}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "devices_count", "value": 30}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "devices_count", "value": 6}
...
```

### Mensajes de batería

El campo `metric` tomara el valor de `battery`.
El campo `value` llevara un porcentaje del nivel de batería desde el `0` hasta el `100`.

```json
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "metric": "battery", "value": 100}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655441111", "metric": "battery", "value": 70}
{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655442222", "metric": "battery", "value": 85}
...
```

### Información de antenas

| property   | description                      | data_type   |  example                                             |
|------------|----------------------------------|-------------|------------------------------------------------------|
| id         | UUID de la antena                | UUID        | `"550e8400-e29b-41d4-a716-446655440000"`             |
| model      | Modelo de la antena              | TEXT        | `"CH-2020"`                                          |
| version    | Version de la antena             | TEXT        | `1.0.0`                                              |
| location   | Coordenadas: latitude, longitude | TEXT        | `39.831741, -3.915119`                               |


### Servicio de monitorización de antenas

* Recolecta las métricas de las antenas y son almacenadas en Apache Kafka en tiempo real.
* Spark Structured Streaming, hace métricas agregadas cada 5 minutos y guarda en PostgreSQL.
    * AVG, MAX, MIN (device_counts) por coordenadas GPS.
    * Antenas con batería inferior al 50%, con sus coordenadas GPS.
    * Antenas que han tenido error (state=-1) en el último intervalo, con sus coordenadas GPS.
* Spark Structured Streaming, también enviara los datos en formato PARQUET a un almacenamiento de google cloud storage, particionado por AÑO, MES, DIA, HORA.

### Servicio de monitorización de antenas

* AVG, MAX, MIN (device_counts) por coordenadas GPS.
* Antenas con batería inferior al 50%, con sus coordenadas GPS.
* % de la hora que una antena ha estado activada, desactivada o con error por ID.