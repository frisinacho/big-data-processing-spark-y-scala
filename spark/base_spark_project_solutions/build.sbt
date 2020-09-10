name := "base_spark_project"

version := "0.1"

scalaVersion := "2.12.12"

val circeVersion = "0.12.3"
val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.apache.spark" %% "spark-core" % 	sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.0" % "test"
)