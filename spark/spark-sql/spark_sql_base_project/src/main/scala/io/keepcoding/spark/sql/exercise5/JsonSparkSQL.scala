package io.keepcoding.spark.sql.exercise5

import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object JsonSparkSQL {
  sealed case class SensorData(sensor_id: Int, temperature: Int, humidity: Int, timestamp: Long)
  val exercise5SensorData = getClass.getClassLoader.getResource("exercise5_sparkcore_data").getFile

  def dataframeAPI(spark: SparkSession): Unit = ???
  def sqlAPI(spark: SparkSession): Unit = ???
  def datasetAPI(spark: SparkSession): Unit = ???

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    dataframeAPI(spark)
    sqlAPI(spark)
    datasetAPI(spark)

    spark.close()
  }

}
