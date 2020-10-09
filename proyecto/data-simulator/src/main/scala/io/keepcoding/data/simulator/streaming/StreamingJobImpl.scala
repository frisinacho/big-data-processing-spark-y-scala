package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.SparkSession

object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  def main(): Unit = run()
}
