# Spark SQL: Exercise 5

## Sensor data with SparkSQL

En este ejercicio vamos a usar los datos del ejercicio5 de SparkCore, 
estos datos se encuentran en la carpeta  `exercise5_sparkcore_data` dentro de la carpeta `resources`
del proyecto y tienen el siguiente formato:

* sensor_id: ID del sensor
* temperature: Medida de la temperatura
* humidity: Medida de la humedad
* timestamp: Unix timestamp en segundos

```json
{"sensor_id":3,"temperature":30,"humidity":80,"timestamp":1599758629}
{"sensor_id":1,"temperature":25,"humidity":80,"timestamp":1599758629}
{"sensor_id":2,"temperature":26,"humidity":59,"timestamp":1599758569}
{"sensor_id":2,"temperature":30,"humidity":72,"timestamp":1599758569}
```

El ejercicio consiste en usar SparkSQL para volver a calcular media de temperatura y humedad,
por sensor en cada minuto, usando:

1. DataFrame API
    * ¿Podría mejorarse el agrupado por fecha? `TimestampType`, `window(...)`
2. SQL Expression
3. DatasetAPI

### Extra

1. Calcular el numero de mensajes medio que reportan entre todos los sensores por minuto.
2. Calcular la temperatura y humedad media por sensor en cada hora.
3. Calcular la temperatura y humedad media por minuto de todos los sensores.
4. Calcular el sensor con la temperatura media más baja de cada minuto.