package io.keepcoding.spark.sql.exercise5

import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object SparkSqlBaseProject {
  val exercise5SensorData = getClass.getClassLoader.getResource("exercise5_sparkcore_data").getFile

  def dataframeAPI(spark: SparkSession): Unit = {
    import spark.implicits._

    spark
      .read
      .format("json")
      .load(s"$exercise5SensorData/*.json")
      .withColumn("timestamp", ($"timestamp" / 60).cast(IntegerType) * 60)
      .groupBy($"timestamp", $"sensor_id")
      .agg(avg($"temperature").as("temperature"), avg($"humidity").as("humidity"))
      .sort("sensor_id", "timestamp")
      .show()

    spark
      .read
      .format("json")
      .load(s"$exercise5SensorData/*.json")
      .groupBy($"sensor_id", window($"timestamp".cast(TimestampType), "1 minute"))
      .agg(avg($"temperature").as("temperature"), avg($"humidity").as("humidity"))
      .withColumn("timestamp", $"window.start".cast(LongType))
      .drop($"window")
      .sort("sensor_id", "timestamp")
      .show()
  }

  def sqlAPI(spark: SparkSession) = {

    spark
      .read
      .format("json")
      .load(s"$exercise5SensorData/*.json")
      .createOrReplaceTempView("sensor_data_view")

    spark.sql(
      """
        |SELECT sensor_id,
        |       CAST(window.start AS INT) AS timestamp,
        |       AVG(humidity) AS humidity,
        |       AVG(temperature) AS temperature
        |FROM sensor_data_view
        |GROUP BY sensor_id, window(CAST(timestamp AS TIMESTAMP), "1 minute")
        |ORDER BY sensor_id, timestamp ASC
      """.stripMargin
    ).show()
  }

  sealed case class SensorData(sensor_id: Int, temperature: Int, humidity: Int, timestamp: Long)

  def datasetAPI(spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema = ScalaReflection.schemaFor[SensorData].dataType.asInstanceOf[StructType]

    spark
      .read
      .schema(schema)
      .format("json")
      .load(s"$exercise5SensorData/*.json")
      .as[SensorData]
      .map { sensor =>
        val roundTimestamp = (sensor.timestamp / 60).toInt * 60
        (sensor.copy(timestamp = roundTimestamp), 1)
      }
      .groupByKey { case (sensor, _) =>
        (sensor.sensor_id, sensor.timestamp)
      }
      .reduceGroups { (sensor1, sensor2) =>
          (
            sensor1._1.copy(
              temperature = sensor1._1.temperature + sensor2._1.temperature,
              humidity = sensor1._1.humidity + sensor2._1.humidity,
            ),
            sensor1._2 + sensor2._2
          )
      }
      .map { case (_, (sensor, count)) =>
        sensor.copy(
          temperature = sensor.temperature / count,
          humidity = sensor.humidity / count
        )
      }
      .sort("sensor_id", "timestamp")
      .show()

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    dataframeAPI(spark)
    sqlAPI(spark)
    datasetAPI(spark)

    spark.close()
  }

}
