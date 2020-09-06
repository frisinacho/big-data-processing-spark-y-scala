
scalaVersion := "2.12.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

lazy val exercise1 = project in file("exercise-1")
lazy val exercise2 = project in file("exercise-2")
lazy val exercise3 = project in file("exercise-3")
lazy val exercise4 = project in file("exercise-4")
lazy val exercise5 = project in file("exercise-5")
lazy val exercise6 = project in file("exercise-6")
lazy val exercise7 = project in file("exercise-7")

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.keepcoding",
        scalaVersion := "2.12.8",
      )
    )
  ).settings(name := "scala-exercise")
  .aggregate(
    exercise1,
    exercise2,
    exercise3,
    exercise4,
    exercise5,
    exercise6,
    exercise7
  )
