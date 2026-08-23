package io.github.adamklosowicz.scd

import io.github.adamklosowicz.scd.exceptions.{DeltaRequiredException, IncompatibleSchemasException}
import io.github.adamklosowicz.scd.utils.{ScdCommonTest, TestHelper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class HelperTest extends ScdCommonTest with BeforeAndAfterEach with Matchers {

  override def afterEach(): Unit = {
    TestHelper.removePath("output")
  }

  test("Should not throw an exception for matching schemas") {
    val targetSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = true),
      StructField("created_at", DateType, nullable = false)
    ))
    val sourceSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = true)
    ))
    val scdSchema: StructType = StructType(Seq(
      StructField("created_at", DateType, nullable = false)
    ))

    val targetDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 10000, "2026-08-01"))), targetSchema)
    val sourceDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 10000))), sourceSchema)

    noException should be thrownBy Helper.validateSchema(targetDf, sourceDf, scdSchema)
  }

  test("Should throw an exception for not matching schemas") {
    val targetSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = true),
      StructField("created_at", DateType, nullable = false)
    ))
    val sourceSchema: StructType = StructType(Seq(
      StructField("employee_id", IntegerType, nullable = false),
      StructField("city", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)
    ))
    val scdSchema: StructType = StructType(Seq(
      StructField("created_at", DateType, nullable = false)
    ))

    val targetDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 10000, "2026-08-01"))), targetSchema)
    val sourceDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, "Wroclaw", 10000))), sourceSchema)

    assertThrows[IncompatibleSchemasException] {
      Helper.validateSchema(targetDf, sourceDf, scdSchema)
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

    assertThrows[DeltaRequiredException] {
      Helper.validateDelta(targetPath)
    }
  }

}
