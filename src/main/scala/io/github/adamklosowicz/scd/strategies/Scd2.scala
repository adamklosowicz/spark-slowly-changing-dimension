package io.github.adamklosowicz.scd.strategies

import io.github.adamklosowicz.scd.Helper.DfExtender
import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.Helper
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

object Scd2 {

  protected val VALID_FROM_COL = "effective_from_date"
  protected val VALID_TO_COL = "effective_to_date"
  protected val IS_CURRENT_COL = "is_current"

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: Option[String] = None,
    isSourceSnapshot: Boolean = false,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    val ingestDateFinal = ingestDate.getOrElse(LocalDate.now().toString)

    if (!DeltaTable.isDeltaTable(spark, targetPath)) {
      saveInitVersion(sourceDf, targetPath, ingestDateFinal)
    } else {
      merge(sourceDf, targetPath, naturalKeyColumns, ingestDateFinal, isSourceSnapshot, technicalColumns)
    }
  }

  protected def saveInitVersion(sourceDf: DataFrame, targetPath: String, ingestDate: String): Unit = {
    val columns: Map[String, Column] = Map(
      VALID_FROM_COL -> lit(ingestDate).cast(DateType),
      VALID_TO_COL -> lit(null).cast(DateType),
      IS_CURRENT_COL -> lit(true)
    )
    sourceDf.saveWithColumns(targetPath, columns)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: String,
    isSourceSnapshot: Boolean,
    technicalColumns: Seq[String]
  )(implicit spark: SparkSession): Unit = {
    val scd2Columns = Seq(VALID_FROM_COL, VALID_TO_COL, IS_CURRENT_COL)
    val target = DeltaTable.forPath(spark, targetPath)
    val targetCurrentDf = target.toDF.filter(col(IS_CURRENT_COL) === true)
      .drop(scd2Columns: _*)

    val columnsToExclude = Seq(
      naturalKeyColumns,
      technicalColumns,
      scd2Columns
    )
    val trackedColumns = Helper.getTrackedColumns(sourceDf, columnsToExclude.flatten)

    val changedSourceDf = sourceDf.alias("source")
      .join(targetCurrentDf.alias("target"), naturalKeyColumns)
      .filter(trackedColumns.map(c => !col(s"source.$c").eqNullSafe(col(s"target.$c"))).reduce(_ || _))
      .select(sourceDf.columns.map(c => col(s"source.$c")): _*)

    val newRecordsDf = sourceDf.join(targetCurrentDf, naturalKeyColumns, "left_anti")

    var mergeSourceDf = changedSourceDf.withColumn("_merge_action", lit("UPDATE"))
      .unionByName(changedSourceDf.withColumn("_merge_action", lit("INSERT")))
      .unionByName(newRecordsDf.withColumn("_merge_action", lit("INSERT")))

    if (isSourceSnapshot) {
      val expiredRecordsDf = targetCurrentDf.join(sourceDf, naturalKeyColumns, "left_anti")
      mergeSourceDf = mergeSourceDf.unionByName(expiredRecordsDf.withColumn("_merge_action", lit("UPDATE")))
    }

    val condition = Helper.getKeyMergeCondition(naturalKeyColumns)
    val mergeCondition = s"""
                       | $condition
                       | AND target.$IS_CURRENT_COL = true
                       | AND source._merge_action = 'UPDATE'
       """.stripMargin

    target.as("target")
      .merge(mergeSourceDf.as("source"), mergeCondition)
      .whenMatched
      .updateExpr(
        Map(
          VALID_TO_COL -> s"to_date('$ingestDate')",
          IS_CURRENT_COL -> "false"
        )
      )
      .whenNotMatched
      .insertExpr(
        sourceDf.columns.map(c => c -> s"source.$c").toMap ++
          Map(
            VALID_FROM_COL -> s"to_date('$ingestDate')",
            VALID_TO_COL -> "NULL",
            IS_CURRENT_COL -> "true"
          )
      )
      .execute
  }

}
