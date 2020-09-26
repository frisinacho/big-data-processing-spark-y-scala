# SSS Exercise 2: Windows

Spark Structured Streaming nos permite trabajar con ventanas temporales, igual que nos lo permite sparkSQL.

En este ejercicio vamos a agregar los datos que copiemos en el directorio `exercise2/input`.

1. Spark añadirá una columna timestamp con el tiempo actual cada vez que procesa un dato.
2. Agregaremos los datos usando la columna de timestamp añadida anteriormente, en una ventana temporal de 5 segundos.
3. Mostraremos la cuenta de palabras en pantalla (OutputMode: Complete)
---
1. Si queremos unicamente mostrar los datos nuevos de cada segundo (OutputMode: Append):
    * Si queremos que los datos para el segundo 1, puedan llegar como muy tarde 5 segundos después, ¿qué podemos hacer?
    