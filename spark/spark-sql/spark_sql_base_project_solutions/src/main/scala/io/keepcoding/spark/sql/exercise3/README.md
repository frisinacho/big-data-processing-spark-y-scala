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

1. En primer lugar vamos a generar un schema AVRO para los datos del instituto.
2. Añade la dependencia de AVRO al sbt para poder trabajar con ella.
    ```scala
   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-avro" % "3.0.1"
   )
    ```
3. Almacena los datos de los alumnos con su categoria del ejercicio anterior en un fichero avro, usando el 
   schema y el writer de avro.
4. Observa el directorio de los datos parquet almacenados. 
    - ¿Puedes ver su contenido? [Viewer](https://plugins.jetbrains.com/plugin/12281-avro-and-parquet-viewer)
5. Una vez ya tenemos nuestra primera version del schema de Avro, vamos a aplicar una evolución.
   Los datos van a soportar una nueva columna de tipo `long` y queremos que pueda ser _nullable_ y su valor por 
   defecto sea `null.`
   - Primero tenemos que definir nuestro schema v1.1.0, soportando la nueva columna.
   - Leemos los datos escritos con la version v1.0.0, que deben de ser compatible y los mostramos con un `show`.
        - ¿Aparece la nueva columna? ¿Tiene el valor por defecto?
   - Podemos añadir la edad a la alumna `sara garcia`, que tiene 18 años y volvemos a escribir los datos, usando el
     nuevo schema.
        - Volvemos a visualizar los datos y comprobamos que son correctos.