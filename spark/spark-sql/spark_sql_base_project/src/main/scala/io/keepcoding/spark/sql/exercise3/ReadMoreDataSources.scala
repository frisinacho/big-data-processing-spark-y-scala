package io.keepcoding.spark.sql.exercise3

import java.nio.file.Files
import scala.io.Source
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object ReadMoreDataSources {
  val exercise2resultPath = getClass.getClassLoader.getResource("exercise2_output").getFile

  def parquet(spark: SparkSession): Unit = {
    val outPath = s"${Files.createTempDirectory("spark-parquet").toFile.getAbsolutePath}/results"

    // <code>

    println(s"Parquet data wrote into: $outPath")

    val outPartitionedPath = s"${Files.createTempDirectory("spark-parquet-partitioned").toFile.getAbsolutePath}/results"

    // <code>

    println(s"Parquet partitioned data wrote into: $outPartitionedPath")
  }

  def avro(spark: SparkSession): Unit = {
    val outPath = s"${Files.createTempDirectory("spark-avro").toFile.getAbsolutePath}/results"

    // <code>

    println(s"Avro data v1.0.0 wrote into: $outPath")

    val outPath110 = s"${Files.createTempDirectory("spark-avro").toFile.getAbsolutePath}/results"

    // <code>

    println(s"Avro data v1.1.0 wrote into: $outPath110")

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    // Read Parquet
    parquet(spark)

    // Read AVRO
    avro(spark)

    spark.close()
  }

}
