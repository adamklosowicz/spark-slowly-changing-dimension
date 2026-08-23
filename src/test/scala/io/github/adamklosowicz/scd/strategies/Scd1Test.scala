package io.github.adamklosowicz.scd.strategies

import io.github.adamklosowicz.scd.utils.{ScdCommonTest, TestHelper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class Scd1Test extends ScdCommonTest with BeforeAndAfterEach with Matchers {

  val schema: StructType = StructType(Seq(
    StructField("employee_id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("salary", IntegerType, nullable = true)
  ))
  val naturalKey: Seq[String] = Seq("employee_id")
  val targetPath: String = "output/target.delta"

  override def beforeEach(): Unit = {
    val initDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 10000),
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Eva", "Wroclaw", 16000)
      )),
      schema
    )
    Scd1(initDf, targetPath, naturalKey, includeDateCol = true, Some("2026-07-01"))
  }

  override def afterEach(): Unit = {
    TestHelper.removePath("output")
  }

  test("SCD1 should update existing record when salary changes") {
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 12500), // salary changed: 10000 -> 12500
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Eva", "Wroclaw", 16000)
      )),
      schema
    )
    Scd1(nextIterationSource, targetPath, naturalKey, includeDateCol = true, Some("2026-08-01"))

    val updatedTarget = spark.read.format("delta").load(targetPath)
    updatedTarget.count() shouldBe  3
    updatedTarget.where(col("employee_id") === 1 && col("salary") === 12500).count() shouldBe 1
  }

  test("SCD1 should create new record when appears in source") {
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 14000), // changed record
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Eva", "Wroclaw", 16000),
        Row(4, "Peter", "London", 12000) // new record
      )),
      schema
    )
    Scd1(nextIterationSource, targetPath, naturalKey, includeDateCol = true, Some("2026-08-01"))

    val updatedTarget = spark.read.format("delta").load(targetPath)
    updatedTarget.count() shouldBe 4
  }

}
