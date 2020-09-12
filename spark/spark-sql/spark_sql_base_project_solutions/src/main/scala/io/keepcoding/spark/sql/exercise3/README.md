# Spark SQL: Exercise 3

## Read More Data Sources

Spark SQL también permite trabajar con datos estructurados binarios como puede ser `PARQUET` y `AVRO`, o fuentes de datos 
externos como una base de datos relaciones (MySQL, PostgresSQL, MariaDB, etc) mediante conexión `JDBC`.

### Parquet

1. Almacena los datos de los alumnos con su categoria del ejercicio anterior en un fichero parquet, usando el writer de parquet.
2. Observa el directorio de los datos parquet almacenados. 
    - ¿Puedes ver su contenido? [Viewer](https://plugins.jetbrains.com/plugin/12281-avro-and-parquet-viewer)
3. Vuelve a almacenar los datos de los alumnos en otro directorio particionando `.partitionBy(...)` por su `curso` y `clase`.
    - Comprueba su contenido, estan los datos particionados.
    - Si visualizamos el fichero de parquet, ¿observamos algún cambio?
    
        Ahora las lecturas también son optimizadas a nivel de particion, si spark decide leer únicamente los datos
        de `curso=humanidades`, solamente se procesaran los ficheros de parquet correspondientes.

### AVRO


