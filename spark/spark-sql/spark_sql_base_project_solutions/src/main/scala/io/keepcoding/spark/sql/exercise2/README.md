# Spark SQL: Exercise 2

## DataFrame Basics

En este ejercicio vamos a realizar una series de transformaciones a un dataset de ejemplo de alumnos
de una escuela.

1. Leemos ambos datasources cada uno con su formato correspondiente JSON & CSV.
2. Cuando tengamos cargados los datos correctamente, unimos ambos dataset para conseguir el dataset completo `unionByName(...)`

Intentaremos contestar las siguientes preguntas usando el dataset:

1. ¿Qué alumno tiene mejor nota media del instituto?
2. ¿Qué clase tiene las peores notas?
3. Añade el tipo de curso en una columna llamada `curso` a cada alumno con las categorias `ciencias`/`humanidades`,
   un alumno que pertenece al curso de ciencias tiene las siguientes clases: `matematicas`, `tecnologia`, `ingles`. 
   Mientras que un alumno de humanidades tiene `filosofia`, `historia`, `ingles`.
   Escribe los resultados en formato JSON en un directorio temporal:
   ```scala
   val outputDir = s"""${Files.createTempDirectory("spark").toFile.getAbsolutePath}/results"""
   ```

Muestra de solución:
1. s
    ```
    +------+--------+----------+
    |nombre|apellido|nota_media|
    +------+--------+----------+
    |  sara|  garcia|       9.0|
    +------+--------+----------+
    ```
2. 
    ```
    +------+----------+
    | clase|nota_media|
    +------+----------+
    |ingles|       6.6|
    +------+----------+
    ```

3.
    ```sql
   +--------+-----------+------+----+-----------+
   |apellido|      clase|nombre|nota|      curso|
   +--------+-----------+------+----+-----------+
   |  garcia|matematicas|  sara|   9|   ciencias|
   |  ferrer|   historia|  juan|  10|humanidades|
   |   ...  |    ...    |  ... | ...|    ...    |
   +--------+-----------+------+----+-----------+
    ```