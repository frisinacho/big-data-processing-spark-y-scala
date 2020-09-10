# Spark Core: Exercise 5

## WordCount

1. Leer los datos del fichero sample.txt usando la función `sc.textFiles(...)`
2. Contar el numero de veces que se repite cada palabra.
3. Imprimir el resultado haciendo un `collect()`

_**Check**: La palabra `keepcoding` se repite un total de 8._

## TopN

1. Leer los datos del fichero sample.txt usando la función `sc.textFiles(...)`
2. Contar el numero de veces que se repite cada palabra.
3. Ordenar las palabras de forma descendiente.
4. Obtener el top5.

_**Check**: La palabra `keepcoding` estará en el top3._

## JsonData

En este ejercicio se procesara un fichero de texto plano, donde se almacena un JSON String por linea. Los JSON contiene información de temperatura y humedad de sensores IoT:

```json
{"sensor_id":3,"temperature":30,"humidity":80,"timestamp":1599758629}
{"sensor_id":1,"temperature":25,"humidity":80,"timestamp":1599758629}
{"sensor_id":2,"temperature":26,"humidity":59,"timestamp":1599758569}
{"sensor_id":2,"temperature":30,"humidity":72,"timestamp":1599758569}
```

* sensor_id: ID del sensor
* temperature: Medida de la temperatura
* humidity: Medida de la humedad
* timestamp: Unix timestamp en segundos

El ejercicio consiste en usar la API de SparkCore para calcular la media de cada sensor para la temperatura y humedad por minuto, el resultado se deberá imprimir por pantalla usando la case class `SensorData`.

1. Leer los datos json desde el fichero de texto plano `data.json` desde los recursos.
2. Decodificar los JSON al modelo `SensorData`, usando la libreria [Circe](https://circe.github.io/circe/) y filtrar los datos decodificados correctamente.
3. Agrupar los datos por sensor y minuto. 
   **NOTA:** Hay que tener en cuenta que las marcas de tiempo vienen en segundo, habría que redondear estas marcas al minuto correspondiente, para poder agrupar.
4. Al agrupar los datos hay que tener en cuenta computar el total de datos por sensor y minuto, para poder calcular la media aritmatica de la humedad y la temperatura.
5. Ordenar los datos por ID de sensor y timestamp de manera ascendente.
6. Recolectar los datos y mostrarlos en pantalla

### EXTRA

Añadir un test unitario usando la libreria [ScalaTest](https://www.scalatest.org/)