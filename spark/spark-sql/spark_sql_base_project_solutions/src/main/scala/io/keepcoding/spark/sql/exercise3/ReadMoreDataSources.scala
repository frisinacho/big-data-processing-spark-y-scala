package io.keepcoding.spark.sql.exercise3

import java.nio.file.Files
import scala.io.Source
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object ReadMoreDataSources {
  val exercise2resultPath = getClass.getClassLoader.getResource("exercise2_output").getFile

  def parquet(spark: SparkSession): Unit = {
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
    val avroSchema = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("avro/highschool_user_schema_v1_0_0.avsc")).mkString
    val outPath = s"${Files.createTempDirectory("spark-avro").toFile.getAbsolutePath}/results"

    spark
      .read
      .format("json")
      .load(s"$exercise2resultPath/*.json")
      .write
      .format("avro")
      .option("avroSchema", avroSchema)
      .save(outPath)

    println(s"Avro data v1.0.0 wrote into: $outPath")

    val avroSchema110 = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("avro/highschool_user_schema_v1_1_0.avsc")).mkString
    val outPath110 = s"${Files.createTempDirectory("spark-avro").toFile.getAbsolutePath}/results"

    import spark.implicits._
    spark
      .read
      .option("avroSchema", avroSchema110)
      .format("avro")
      .load(outPath)
      .withColumn("edad", when($"nombre" === "sara" && $"apellido" === "garcia", 18L))
      .write
      .format("avro")
      .option("avroSchema", avroSchema110)
      .save(outPath110)

    println(s"Avro data v1.1.0 wrote into: $outPath110")

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    // Read Parquet
    // parquet(spark)

    // Read AVRO
    avro(spark)

    spark.close()
  }

}
