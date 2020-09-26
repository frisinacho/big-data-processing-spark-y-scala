# SSS Exercise 3: Joins

Spark Structured Streaming nos permite hacer joins:
* stream <-> batch
* stream <-> stream (en un rango de tiempo)

## Stream <-> Batch

En este ejercicio vamos a cardar un dataFrame batch donde tenemos metadatos de los sensores:

```csv
sensor_id|location|maintainer
1|dormitorio|andres@gmail.com
2|salon|andres@gmail.com
3|salon|juan@gmail.com
```

y un flujo de datos en tiempo real:

```json
{"sensor_id":3,"temperature":30,"humidity":80,"timestamp":1599758629}
{"sensor_id":1,"temperature":25,"humidity":80,"timestamp":1599758629}
{"sensor_id":2,"temperature":26,"humidity":59,"timestamp":1599758569}
{"sensor_id":2,"temperature":30,"humidity":72,"timestamp":1599758569}
{"sensor_id":2,"temperature":27,"humidity":73,"timestamp":1599758629}
{"sensor_id":2,"temperature":31,"humidity":62,"timestamp":1599758569}
```


1. Queremos obtener un stream de salida enriquecido con la información estática de cada sensor.

## Stream <-> Stream

En este ejercicio vamos a plantear un escenario donde queremos hacer join entre dos flujos de mensajes. En concreto,
nos encontramos dos tipos de flujos provenientes de una red WIFI.

1. Mensajes de aplicación de dispositivos

    ```json
    {"timestamp":1599758629, "mac": "00:00:00:00:00:00", "app":  "skype", "bytes": 230}
    {"timestamp":1599758629, "mac": "00:00:00:00:00:01", "app":  "whatsapp", "bytes": 1500}
    {"timestamp":1599758689, "mac": "00:00:00:00:00:00", "app":  "facetime", "bytes": 10000}
    ...
    ```
   
2. Mensajes de localización de dispositivos

    ```json
    {"timestamp":1599758629, "mac": "00:00:00:00:00:00", "location":  "Gran_Plaza"}
    {"timestamp":1599758629, "mac": "00:00:00:00:00:01", "location":  "Hall_1"}
    {"timestamp":1599758689, "mac": "00:00:00:00:00:00", "location":  "Pabellon_27"}
    ...
    ```

* Sabemos que los mensajes de localización suelen llegar unos segundos más tarde que los de actividad.
* No queremos ubicar un usuario si su mensaje se retrasa más de 30 segundos.

1. [Console] Stream de datos de: "Usuarios distintos que usan una aplicación concreta en cada zona, agregado por minuto"