package io.github.adamklosowicz.scd.strategies

import io.github.adamklosowicz.scd.exceptions.DeltaRequiredException
import io.github.adamklosowicz.scd.utils.{ScdCommonTest, TestHelper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class Scd3Test extends ScdCommonTest with BeforeAndAfterEach with Matchers {

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
    Scd3(initDf, targetPath, naturalKey, Some("2026-07-01"))
  }

  override def afterEach(): Unit = {
    TestHelper.removePath("output")
  }

  test("SCD3 should update existing record when salary changes and add new record  when appears in source") {
    val nextIterationSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "Adam", "Berlin", 12500), // salary changed: 10000 -> 12500
        Row(2, "John", "Warsaw", 12000),
        Row(3, "Eva", "Wroclaw", 16000),
        Row(4, "Alice", "London", 15000) // new record
      )),
      schema
    )
    Scd3(nextIterationSource, targetPath, naturalKey, Some("2026-08-01"))

    val updatedTarget = spark.read.format("delta").load(targetPath)
    updatedTarget.count() shouldBe 4

    val records = updatedTarget.orderBy("employee_id").collect()
    assert(records(0).getAs[Int]("salary") === 12500)
    assert(records(0).getAs[Int]("salary_previous") === 10000)
    assert(records(0).getAs[Date]("created_on").toString === "2026-07-01")
    assert(records(0).getAs[Date]("previous_updated_on").toString === "2026-07-01")
    assert(records(0).getAs[Date]("last_updated_on").toString === "2026-08-01")

    assert(records(1).getAs[Date]("previous_updated_on") === null)
    assert(records(2).getAs[Date]("previous_updated_on") === null)

    assert(records(3).getAs[Int]("salary") === 15000)
    assert(records(3).getAs[Int]("salary_previous") === null)
    assert(records(3).getAs[Date]("created_on").toString === "2026-08-01")
    assert(records(3).getAs[Date]("previous_updated_on") === null)
    assert(records(3).getAs[Date]("last_updated_on").toString === "2026-08-01")
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
      Scd3(nextIterationDf, targetPath, naturalKey, Some("2026-08-01"))
    }
  }

}
