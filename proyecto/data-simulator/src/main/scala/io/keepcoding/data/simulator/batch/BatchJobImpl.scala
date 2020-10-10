package io.keepcoding.data.simulator.batch
import org.apache.spark.sql.SparkSession

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._
}
