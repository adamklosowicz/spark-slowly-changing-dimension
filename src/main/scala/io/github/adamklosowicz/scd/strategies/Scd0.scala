package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.utils.{ScdCommon, ScdValidator}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

object Scd0 extends ScdCommon with ScdValidator {

  protected val CREATED_ON_COL = "created_on"

  protected val SCD0_SCHEMA = StructType(Seq(
    StructField(CREATED_ON_COL, DateType, nullable = false)
  ))

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    if (!pathExists(targetPath)) {
      saveInitVersion(sourceDf, targetPath, includeDateCol, ingestDate)
    } else {
      merge(sourceDf, targetPath, naturalKeyColumns, includeDateCol, ingestDate)
    }
  }

  protected def saveInitVersion(
    sourceDf: DataFrame,
    targetPath: String,
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  ): Unit = {
    val columns: Map[String, Column] = if (includeDateCol) {
      Map(CREATED_ON_COL -> lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType))
    } else Map()
    sourceDf.saveWithColumns(targetPath, columns)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    validateDelta(targetPath)
    val target = DeltaTable.forPath(spark, targetPath)

    val scdSchema = if (includeDateCol) SCD0_SCHEMA else StructType(Seq())
    validateSchema(target.toDF, sourceDf, scdSchema)

    val mergeCondition = getMergeCondition(naturalKeyColumns)
    val technicalMap = if (includeDateCol) {
      val ingestDateStr = ingestDate.getOrElse(LocalDate.now())
      Map(CREATED_ON_COL -> s"to_date('$ingestDateStr')")
    } else Map()

    target.as(TARGET_ALIAS)
      .merge(sourceDf.as(SOURCE_ALIAS), mergeCondition)
      .whenNotMatched
      .insertExpr(sourceDf.columns.map(c => c -> s"$SOURCE_ALIAS.$c").toMap ++ technicalMap)
      .execute
  }

}

