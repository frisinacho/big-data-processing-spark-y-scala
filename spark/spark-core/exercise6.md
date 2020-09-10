# Spark Core Exercise 6: Spark UI

* Modificar el ejercicio de procesamiento de JSON, para realizar un foreach final, donde por cada registro se realiará un sleep de 10 segundos `Thread.sleep(10000)` y se imprimira por pantalla:

```scala
.foreach(sensorData => {
    print("SLEEP ---> ")
    Thread.sleep(10000)
    println(sensorData)
})
```

Esto nos permitira comprobar en la salida de la ejecucción el puerto donde se activa la SparkUI y analizar el procesamiento de los datos en la interfaz web.