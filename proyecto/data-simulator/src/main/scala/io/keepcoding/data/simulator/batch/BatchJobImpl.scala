package io.keepcoding.data.simulator.batch
import java.time.OffsetDateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, year: String, month: String, day: String, hour: String): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === year &&
          $"month" === month &&
          $"day" === day &&
          $"hour" === hour
      )
  }

  def main(args: Array[String]): Unit = run(args)
}
