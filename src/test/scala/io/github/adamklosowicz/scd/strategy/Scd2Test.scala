package io.github.adamklosowicz.scd.strategy

import io.github.adamklosowicz.scd.utils.ScdCommonTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.apache.hadoop.fs.{FileSystem, Path}

class Scd2Test extends ScdCommonTest with BeforeAndAfterEach {

  val schema: StructType = StructType(Seq(
    StructField("employee_id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("salary", IntegerType, nullable = true)
  ))
  val targetPath = "output/target.delta"

  override def beforeEach(): Unit = {
    val initDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 10000),
        Row(2, "John", "Warsaw", 12000)
      )),
      schema
    )
    Scd2(initDf, targetPath, Seq("employee_id"), "2026-07-01")
  }

  override def afterEach(): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path("output"), true)
  }

  test("SCD2 should create new version when salary changes or new record appears in source") {
    val newSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 12500), // salary changed: 10000 -> 12500
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Alice", "London", 15000) // new record
      )),
      schema
    )
    Scd2(newSource, targetPath, Seq("employee_id"), "2026-08-01")

    val newTarget = spark.read.format("delta").load(targetPath)
    newTarget.count() equals 4
  }
}
