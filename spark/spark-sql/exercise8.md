# Spark SQL: Exercise 8

## spark-sql CLI

En este ejercicio vamos a usar la CLI de `spark-sql` que viene con la distribución de spark, para volver 
a procesar los datos de sensores pero sin utilizar nada de código fuente.

1. Inicializamos la CLI que encontramos en `bin/spark-sql`, dentro de la distrubición de spark.
2. Primero vamos a crear una tabla a partir de una ruta, copiamos la ruta absoluta desde el IDE de desarrollo:
    ```sql
    CREATE TABLE sensor_data (timestamp BIGINT, sensor_id INT, temperature INT, humidity INT)
    USING org.apache.spark.sql.json 
     OPTIONS(
         path "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-sql/spark_sql_base_project_solutions/src/main/resources/exercise5_sparkcore_data/*.json"
     );
    ```
3. Podemos describir la tabla generada para ver sus columnas y tipos:
    ```sql
    DESCRIBE TABLE sensor_data;
    ```
4. Lanzamos la consulta SQL que hemos creado en el ejercicio anterior:
    ```sql
    SELECT sensor_id,
        CAST(window.start AS INT) AS timestamp,
        AVG(humidity) AS humidity,
        AVG(temperature) AS temperature
    FROM sensor_data
    GROUP BY sensor_id, window(CAST(timestamp AS TIMESTAMP), "1 minute")
    ORDER BY sensor_id, timestamp ASC;
    ```
5. Finalmente vamos a escribir los resultados en un fichero en una ruta de nuestra elección:
    ```sql 
    INSERT OVERWRITE DIRECTORY  USING json
    OPTIONS ('path' '/Users/agf/sql-test')
        SELECT sensor_id,
            CAST(window.start AS INT) AS timestamp,
            AVG(humidity) AS humidity,
            AVG(temperature) AS temperature
        FROM sensor_data
        GROUP BY sensor_id, window(CAST(timestamp AS TIMESTAMP), "1 minute")
        ORDER BY sensor_id, timestamp ASC;
    ```