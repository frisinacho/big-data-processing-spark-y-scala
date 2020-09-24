# Exercise 6: Built-in Functions

En este ejercicio vamos a trabajar usando distintas funciones que existen en SparkSQL. La principal idea es que sepáis
donde se encuentran las funciones y como aplicar algunas de ellas, para poder buscar la que necesitéis en cada momento.

[SparkSQL API](https://spark.apache.org/docs/latest/api/sql/index.html)

## Arrays

Leer los datos que contiene la carpeta de `exercise6/arrays.json` en el directorio resources.

```json
{"sensor_id":1,"metrics":[26, 74],"timestamp":1599758629, "locations":["ZonaA", "Pasillo1"]}
{"sensor_id":2,"metrics":[31, 80],"timestamp":1599758629, "locations":["ZonaB", "Pasillo1"]}
{"sensor_id":3,"metrics":[34, 60],"timestamp":1599758629, "locations":["ZonaA", "Pasillo2"]}
```

1. Pintar el schema 
2. Obtener el max y min de las métricas por sensor y minuto
3. Obtener los datos pertenecientes a la ZonaA
4. Si sabemos que:
    * La posicion 0 de metrics es igual a temperatura
    * La posicion 1 de metrics es igual a humedad
    * La posicion 0 de locations es zona
    * La posicion 1 de locations es pasillo
    
    Podemos generar una tabla con esas columnas.
  
## Complex

Leer los datos que contiene la carpeta de `exercise6/complex.json` en el directorio resources.

```
{"source":"Server001", "timestamp":1599758689, "messages": [{"sensor_id":1, "data": {"temperature": 28, "humidity": 80}}, {"sensor_id":2, "data": {"temperature": 28, "humidity": 80}}, {"sensor_id":3, "data": {"temperature": 28, "humidity": 80}}]
{"source":"Server001", "timestamp":1599758749, "messages": [{"sensor_id":1, "data": {"temperature": 32, "humidity": 92}}, {"sensor_id":2, "data": {"temperature": 31, "humidity": 82}}, {"sensor_id":3, "data": {"temperature": 25, "humidity": 70}}]

```

1. Pintar el schema 
2. Calcular la media de humedad y temperatura por sensor y minuto
