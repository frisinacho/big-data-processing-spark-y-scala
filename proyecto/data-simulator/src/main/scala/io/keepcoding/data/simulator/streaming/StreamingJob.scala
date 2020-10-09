package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

    def run(args: Array[String]): Unit = {
      val Array(kafkaServer, topic) = args
      println(s"Running with: ${args.toSeq}")

      val kafkaDF = readFromKafka(kafkaServer, topic)
      val devicesDF = parserJsonData(kafkaDF)

      devicesDF
        .writeStream
        .format("console")
        .start()
        .awaitTermination()



      spark.close()

      // ARGS: KAFKA_SERVER:9092 devices
    }

}
