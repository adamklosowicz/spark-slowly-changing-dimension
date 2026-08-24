package io.github.adamklosowicz.scd.strategies

import io.github.adamklosowicz.scd.exceptions.{DeltaRequiredException, IncompatibleSchemasException}
import io.github.adamklosowicz.scd.utils.{ScdCommonTest, TestHelper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class Scd2Test extends ScdCommonTest with BeforeAndAfterEach with Matchers {

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
    Scd2(initDf, targetPath, naturalKey, Some("2026-07-01"))
  }

  override def afterEach(): Unit = {
    TestHelper.removePath("output")
  }

  test("SCD2 should create new version when salary changes or new record appears in source") {
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 12500), // salary changed: 10000 -> 12500
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Eva", "Wroclaw", 16000),
        Row(4, "Alice", "London", 15000) // new record
      )),
      schema
    )
    Scd2(nextIterationSource, targetPath, naturalKey, Some("2026-08-01"))

    val updatedTarget = spark.read.format("delta").load(targetPath)
    updatedTarget.count() shouldBe 5
  }

  test("SCD2 should deactivate record that disappears in source") {
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 10000),
        // Row(2, "John", "Warsaw", 12000), removed record in source snapshot
        Row(3, "Eva", "Wroclaw", 16000)
      )),
      schema
    )
    Scd2(nextIterationSource, targetPath, naturalKey, Some("2026-08-01"), isSourceSnapshot = true)

    val updatedTarget = spark.read.format("delta").load(targetPath)
    updatedTarget.count() shouldBe 3
    updatedTarget.where(col("employee_id") === 2 && !col("is_current")).count() shouldBe 1
  }

  test("Should throw an exception for not matching schemas") {
    val wrongSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)
    ))
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", 12500),
        Row(2, "John", 12000)
      )),
      wrongSchema
    )
    assertThrows[IncompatibleSchemasException] {
      Scd2(nextIterationSource, targetPath, naturalKey, Some("2026-08-01"))
    }
  }

  test("Should throw an exception when file is not delta") {
    val targetPath = "output/fake.delta"
    val targetSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("city", StringType, nullable = false),
      StructField("salary", IntegerType, nullable = true)
    ))
    val targetDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, "Wroclaw", 10000))), targetSchema)
    targetDf.write.mode("overwrite").parquet(targetPath)

    val nextIterationDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(2, "Berlin", 12000))), targetSchema)

    assertThrows[DeltaRequiredException] {
      Scd2(nextIterationDf, targetPath, naturalKey, Some("2026-08-01"))
    }
  }

}
