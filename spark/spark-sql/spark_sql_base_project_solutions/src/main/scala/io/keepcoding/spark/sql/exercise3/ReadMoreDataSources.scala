package io.keepcoding.spark.sql.exercise3

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

object ReadMoreDataSources {

  def parquet(spark: SparkSession): Unit = {
    val exercise2resultPath = getClass.getClassLoader.getResource("exercise2_output").getFile
    val outPath = s"${Files.createTempDirectory("spark-parquet").toFile.getAbsolutePath}/results"

    spark
      .read
      .format("json")
      .load(s"$exercise2resultPath/*.json")
      .write
      .format("parquet")
      .save(outPath)

    println(s"Parquet data wrote into: $outPath")

    val outPartitionedPath = s"${Files.createTempDirectory("spark-parquet-partitioned").toFile.getAbsolutePath}/results"

    spark
      .read
      .format("json")
      .load(s"$exercise2resultPath/*.json")
      .write
      .partitionBy("curso", "clase")
      .format("parquet")
      .save(outPartitionedPath)

    println(s"Parquet partitioned data wrote into: $outPartitionedPath")
  }

  def avro(spark: SparkSession): Unit = {

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
