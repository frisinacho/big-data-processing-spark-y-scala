# Exercise 5: Implicits

Queremos extender la funcionalidad del tipo `String`
Queremos añadir un método `toPassword`, para remplazar algunas vocales por números:

- a = 4
- e = 3
- i = 1
- o = 0

Finalmente, queremos usarlo como se ve a continuación:

```scala
"Hello World!".toPassword  //H3ll0 W0rld!
``` 

Comprueba los resultados con los test unitarios.