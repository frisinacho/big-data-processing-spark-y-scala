package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  def main(args: Array[String]): Unit = run(args)
}
