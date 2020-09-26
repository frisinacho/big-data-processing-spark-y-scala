# SSS Exercise 5: Kafka Integration

Spark Structured Streaming permite trabajar con Apache Kafka cono data format.

1. Necesitamos añadir una nueva dependencia al sbt, para poder usar este format:

    ```
      "org.apache.spark" %% "spark-sql-kafka-0-10_2.12" % sparkVersion,
      "org.apache.kafka" % "kafka-clients" % "2.6.0"
    ```
    
    También añadimos la dependencia de los clientes de Kafka, para utilizar la ultima versión.
    
2. Necesitamos crear un broker de Kafka, podemos seguir la guia [Kafka Setup](../../../../../../../../../../../../proyecto/vm_setup.md)
    * Configurar instancía de debian (1-4)
    * Ejecutar Apache Kafka


## Parte 1
1. Leer de un topic de kafka llamado `test`, imprimir el schema del dataFrame y enviar la salida usando el sink de `console`.

* ¿Cómo es el schema? ¿Cuál es el mensaje que estamos enviando?

## Parte 2
1. Hacer un cast de la columna `value` a StringType. ¿Vemos nuestro mensaje?
2. Si ahora tenemos un mensaje en formato JSON String y lo enviamos a Kafka, como podríamos obtener las columnas en Spark Structured Streaming.
    ```json
    {"id":"12345", "name":"andres"}
    ```
## Parte 3
El conector de kafka nos permite también leer desde kafka en formato batch.

* Lee el topic de kafka en formato batch, desde el inicio hasta el fin.

## Parte 4
Utilizar Kafka como sink, para enviar  el `id` de los mensajes JSON usados anteriormente, como KAFKA_KEY y el `name`
como KAFKA_VALUE, al topic de salida `out`

## Parte 5
Kafka nos permite cambiar el modo de configuración de spark a Continuos Processing, lo que nos permite latencias mucho más bajas
pero únicamente soportamos at-most-once.

