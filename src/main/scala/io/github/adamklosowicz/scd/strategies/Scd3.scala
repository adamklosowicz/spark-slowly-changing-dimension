package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.utils.{ScdCommon, ScdValidator}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

object Scd3 extends ScdCommon with ScdValidator {

  protected val CREATED_ON_COL = "created_on"
  protected val PREVIOUS_UPDATED_ON_COL = "previous_updated_on"
  protected val LAST_UPDATED_ON_COL = "last_updated_on"
  protected val PREVIOUS_COL_SUFFIX = "_previous"

  protected val SCD3_SCHEMA = StructType(Seq(
    StructField(CREATED_ON_COL, DateType, nullable = false),
    StructField(PREVIOUS_UPDATED_ON_COL, DateType, nullable = true),
    StructField(LAST_UPDATED_ON_COL, DateType, nullable = false)
  ))

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: Option[String] = None,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    val ingestDateFinal = ingestDate.getOrElse(LocalDate.now().toString)

    if (!pathExists(targetPath)) {
      saveInitVersion(sourceDf, targetPath, naturalKeyColumns, ingestDateFinal, technicalColumns)
    } else {
      merge(sourceDf, targetPath, naturalKeyColumns, ingestDateFinal, technicalColumns)
    }
  }

  protected def saveInitVersion(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String]
  ): Unit = {
    sourceDf.cloneTrackedColumns(naturalKeyColumns, technicalColumns, PREVIOUS_COL_SUFFIX)
      .saveWithColumns(targetPath, getDateColumns(ingestDate))
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String]
  )(implicit spark: SparkSession): Unit = {
    validateDelta(targetPath)
    val target = DeltaTable.forPath(spark, targetPath)
    val columnsToExclude = Seq(
      naturalKeyColumns,
      technicalColumns,
      SCD3_SCHEMA.fields.map(_.name).toSeq
    )
    val trackedColumns = getTrackedColumns(sourceDf, columnsToExclude.flatten)
    val previousColumns: Array[StructField] = trackedColumns.map(name => StructField(name + PREVIOUS_COL_SUFFIX, sourceDf.schema(name).dataType)).toArray
    validateSchema(target.toDF, sourceDf, StructType(SCD3_SCHEMA.fields ++ previousColumns))

    val enrichedSourceDf = sourceDf.cloneTrackedColumns(naturalKeyColumns, technicalColumns, PREVIOUS_COL_SUFFIX)
      .applyColumns(getDateColumns(ingestDate))

    val mergeCondition = getMergeCondition(naturalKeyColumns)
    val matchCondition = getMergeCondition(trackedColumns, "<=>", "OR", "NOT (<condition>)")
    val updateMap = trackedColumns.flatten(c => Map(s"$c$PREVIOUS_COL_SUFFIX" -> s"$TARGET_ALIAS.$c", c -> s"$SOURCE_ALIAS.$c")).toMap
    val technicalMap = Map(
      PREVIOUS_UPDATED_ON_COL -> s"$TARGET_ALIAS.$LAST_UPDATED_ON_COL",
      LAST_UPDATED_ON_COL -> s"to_date('$ingestDate')"
    )
    target.as(TARGET_ALIAS)
      .merge(enrichedSourceDf.as(SOURCE_ALIAS), mergeCondition)
      .whenMatched(matchCondition)
      .updateExpr(updateMap ++ technicalMap)
      .whenNotMatched
      .insertExpr(enrichedSourceDf.columns.map(c => c -> s"$SOURCE_ALIAS.$c").toMap)
      .execute
  }

  protected def getDateColumns(ingestDate: String): Map[String, Column] =
    Map(
      CREATED_ON_COL -> lit(ingestDate).cast(DateType),
      PREVIOUS_UPDATED_ON_COL -> lit(null).cast(DateType),
      LAST_UPDATED_ON_COL -> lit(ingestDate).cast(DateType)
    )

}
