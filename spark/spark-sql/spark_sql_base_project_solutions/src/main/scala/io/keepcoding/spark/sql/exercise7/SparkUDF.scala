package io.keepcoding.spark.sql.exercise7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext
      .parallelize(Seq("Andres", "Pepe", "Juan"))
      .toDF("name")
      .createTempView("name_table")

    /*
    1. Usando los datos de la tabla `name_table`, escribe una función UDF que transforme
       a upperCase las nombres que sean igual a 'Andres', ejecuta la sentencia usando sintasix SQL.
     */

    spark.udf.register("upperCaseAndres", udf((x: String) => if(x == "Andres") x.toUpperCase else x))

    spark.sql("SELECT upperCaseAndres(name) FROM name_table")
      .show()

    spark.close()
  }

}
