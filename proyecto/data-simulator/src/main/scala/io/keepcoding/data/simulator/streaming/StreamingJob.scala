package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.{DataFrame, SparkSession}

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

    def run(args: Array[String]): Unit = {
      val Array(kafkaServer, topic) = args

      val kafkaDF = readFromKafka(kafkaServer, topic)

      println("Hello world!")
    }

}
