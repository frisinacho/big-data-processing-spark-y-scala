package io.keepcoding.spark.sql.exercise4

import io.keepcoding.spark.sql.exercise3.ReadMoreDataSources.exercise2resultPath
import org.apache.spark.sql.{SaveMode, SparkSession}

object JDBC {
  val IpServer = "34.71.29.40"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    val psqlDF = spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$IpServer:5432/postgres")
      .option("dbtable", "instituto")
      .option("user", "postgres")
      .option("password", "keepcoding")
      .load()
      .cache()

    psqlDF.show()

    val oldDF = spark
      .read
      .format("json")
      .load(s"$exercise2resultPath/*.json")

    psqlDF
      .unionByName(oldDF)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$IpServer:5432/postgres")
      .option("dbtable", "instituto")
      .option("user", "postgres")
      .option("password", "keepcoding")
      .save()

    spark.close()
  }

}
