ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-slowly-changing-dimension"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.4" % "provided",
  "io.delta" %% "delta-spark" % "3.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)