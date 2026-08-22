package io.github.adamklosowicz.scd.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

class ScdCommonTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Scd2Test")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

}
