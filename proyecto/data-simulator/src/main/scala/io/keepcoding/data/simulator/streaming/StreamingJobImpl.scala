package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema: StructType = ScalaReflection.schemaFor[DevicesMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("value").cast(StringType), schema).as("json"))
      .select("json.*")
  }

  def main(args: Array[String]): Unit = run(args)
}
