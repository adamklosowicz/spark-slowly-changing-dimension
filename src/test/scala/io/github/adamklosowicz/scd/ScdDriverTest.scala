package io.github.adamklosowicz.scd

import io.github.adamklosowicz.scd.exceptions.InvalidScdTypeException
import io.github.adamklosowicz.scd.utils.{ScdCommonTest, TestHelper}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class ScdDriverTest extends ScdCommonTest with BeforeAndAfterEach with Matchers {

  var df: DataFrame = _
  val schema: StructType = StructType(Seq(
    StructField("employee_id", IntegerType, nullable = false),
    StructField("salary", IntegerType, nullable = true)
  ))
  val naturalKey: Seq[String] = Seq("employee_id")
  val targetPath: String = "output/target.delta"

  override def beforeAll(): Unit = {
    super.beforeAll()

    df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 10000),
        Row(2, 12000)
      )),
      schema
    )
  }

  override def afterEach(): Unit = {
    TestHelper.removePath("output")
  }

  test("Should not throw an exception for a valid SCD type") {
    noException should be thrownBy ScdDriver("SCD2", df, targetPath, naturalKey, Some("2026-08-01"))
  }

  test("Should throw an exception for an invalid SCD type") {
    val ex = intercept[InvalidScdTypeException] {
      ScdDriver("invalid_scd", df, targetPath, naturalKey, Some("2026-08-01"))
    }
    ex.getMessage shouldBe "Invalid SCD type: invalid_scd"
  }

}
