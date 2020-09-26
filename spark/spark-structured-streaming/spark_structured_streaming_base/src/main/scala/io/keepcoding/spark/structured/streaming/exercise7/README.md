# SSS Exercise 7: DataProc - Sensor Data Realtime Aggregation Job

Este ejercicio consiste en ejecutar la agregación de datos de sensores. Usaremos los mensajes de los ejercicios anteriores:

```json
{"sensor_id":3,"temperature":30,"humidity":80,"timestamp":1599758629}
{"sensor_id":1,"temperature":25,"humidity":80,"timestamp":1599758629}
{"sensor_id":2,"temperature":26,"humidity":59,"timestamp":1599758569}
{"sensor_id":2,"temperature":30,"humidity":72,"timestamp":1599758569}
{"sensor_id":2,"temperature":27,"humidity":73,"timestamp":1599758629}
{"sensor_id":2,"temperature":31,"humidity":62,"timestamp":1599758569}
{"sensor_id":3,"temperature":30,"humidity":80,"timestamp":1599758629}
{"sensor_id":2,"temperature":30,"humidity":75,"timestamp":1599758629}
```

El job de spark:

1. Leera los mensajes desde un topic de kafka llamado: `sensor_data`
2. Calculara la media de temperatura y humedad por minuto, por sensor
3. Enviara los resultados a un topic de kafka llamada: `sensor_agg_data`

**Notas:**
* Un mensaje que llegue 30 segundos después de la ventana del minuto será descartado.
* Reproduce los mensajes en kafka por lotes, mirar fichero `resources/exercise9/sensor_data.json`