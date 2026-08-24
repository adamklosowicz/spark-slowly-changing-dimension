package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.utils.{ScdCommon, ScdValidator}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

object Scd1 extends ScdCommon with ScdValidator {

  protected val CREATED_AT_COL = "created_at"
  protected val UPDATED_AT_COL = "updated_at"

  protected val SCD1_SCHEMA = StructType(Seq(
    StructField(CREATED_AT_COL, DateType, nullable = false),
    StructField(UPDATED_AT_COL, DateType, nullable = false)
  ))

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    if (!pathExists(targetPath)) {
      saveInitVersion(sourceDf, targetPath, includeDateCol, ingestDate)
    } else {
      merge(sourceDf, targetPath, naturalKeyColumns, includeDateCol, ingestDate, technicalColumns)
    }
  }

  protected def saveInitVersion(
    sourceDf: DataFrame,
    targetPath: String,
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  ): Unit = {
    val columns: Map[String, Column] = if (includeDateCol) {
      Map(
        CREATED_AT_COL -> lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType),
        UPDATED_AT_COL -> lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType)
      )
    } else Map()
    sourceDf.saveWithColumns(targetPath, columns)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    validateDelta(targetPath)
    val target = DeltaTable.forPath(spark, targetPath)

    val scdSchema = if (includeDateCol) SCD1_SCHEMA else StructType(Seq())
    validateSchema(target.toDF, sourceDf, scdSchema)

    var columnsToExclude = Seq(
      naturalKeyColumns,
      technicalColumns
    )
    if (includeDateCol) {
      columnsToExclude = columnsToExclude ++ Seq(Seq(CREATED_AT_COL, UPDATED_AT_COL))
    }
    val trackedColumns = getTrackedColumns(sourceDf, columnsToExclude.flatten)

    val mergeCondition = getMergeCondition(naturalKeyColumns)
    val matchCondition = getMergeCondition(trackedColumns, "<=>", "OR", "NOT (<condition>)")
    val technicalMap1: Map[String, String] = if (includeDateCol) {
      val ingestDateStr = ingestDate.getOrElse(LocalDate.now())
      Map(UPDATED_AT_COL -> s"to_date('$ingestDateStr')")
    } else Map()
    val technicalMap2: Map[String, String] = if (includeDateCol) {
      technicalMap1 ++ Map(CREATED_AT_COL -> technicalMap1(UPDATED_AT_COL))
    } else Map()

    target.as("target")
      .merge(sourceDf.as("source"), mergeCondition)
      .whenMatched(matchCondition)
      .updateExpr(sourceDf.columns.map(c => c -> s"source.$c").toMap ++ technicalMap1)
      .whenNotMatched
      .insertExpr(sourceDf.columns.map(c => c -> s"source.$c").toMap ++ technicalMap2)
      .execute
  }

}

