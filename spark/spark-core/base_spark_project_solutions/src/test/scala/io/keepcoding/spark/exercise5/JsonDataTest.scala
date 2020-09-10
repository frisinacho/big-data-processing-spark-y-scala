package io.keepcoding.spark.exercise5

import org.scalatest._
import flatspec._
import io.keepcoding.spark.exercise5.JsonDataReadyForTesting.SensorData
import matchers._
import org.apache.spark.{SparkConf, SparkContext}

class JsonDataTest extends AnyFlatSpec with should.Matchers {

  "Spark" should "read json data and compute average metrics value per minute" in {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sampleDataRDD = sc.parallelize(Seq(
      """{"sensor_id":1,"temperature":10,"humidity":30,"timestamp":1000000005}""".stripMargin,
      """{"sensor_id":1,"temperature":20,"humidity":50,"timestamp":1000000001}""".stripMargin,

      """{"sensor_id":1,"temperature":10,"humidity":30,"timestamp":1000000062}""".stripMargin,

      """{"sensor_id":2,"temperature":10,"humidity":20,"timestamp":1000000063}""".stripMargin,
      """{"sensor_id":2,"temperature":10,"humidity":30,"timestamp":1000000062}""".stripMargin,
    ))

    val results = JsonDataReadyForTesting.processJsonData(sampleDataRDD).collect().toSeq

    results.size should be (3)
    results should be(Seq(
      SensorData(1, 15, 40, 999999960L),
      SensorData(1, 10, 30, 1000000020L),
      SensorData(2, 10, 25, 1000000020L)
    ))

    sc.stop()
  }
}
