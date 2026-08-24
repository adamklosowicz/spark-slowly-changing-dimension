package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.utils.{ScdCommon, ScdValidator}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BooleanType, DateType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

object Scd2 extends ScdCommon with ScdValidator {

  protected val VALID_FROM_COL = "effective_from_date"
  protected val VALID_TO_COL = "effective_to_date"
  protected val IS_CURRENT_COL = "is_current"

  protected val SCD2_SCHEMA = StructType(Seq(
    StructField(VALID_FROM_COL, DateType, nullable = false),
    StructField(VALID_TO_COL, DateType, nullable = true),
    StructField(IS_CURRENT_COL, BooleanType, nullable = false)
  ))

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: Option[String] = None,
    isSourceSnapshot: Boolean = false,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    val ingestDateFinal = ingestDate.getOrElse(LocalDate.now().toString)

    if (!pathExists(targetPath)) {
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
    validateDelta(targetPath)

    val target = DeltaTable.forPath(spark, targetPath)
    validateSchema(target.toDF, sourceDf, SCD2_SCHEMA)

    val targetCurrentDf = target.toDF.filter(col(IS_CURRENT_COL) === true)
      .drop(SCD2_SCHEMA.fields.map(_.name).toSeq: _*)

    val columnsToExclude = Seq(
      naturalKeyColumns,
      technicalColumns,
      SCD2_SCHEMA.fields.map(_.name).toSeq
    )
    val trackedColumns = getTrackedColumns(sourceDf, columnsToExclude.flatten)
    val changedSourceDf = sourceDf.alias(SOURCE_ALIAS)
      .join(targetCurrentDf.alias(TARGET_ALIAS), naturalKeyColumns)
      .filter(trackedColumns.map(c => !col(s"$SOURCE_ALIAS.$c").eqNullSafe(col(s"$TARGET_ALIAS.$c"))).reduce(_ || _))
      .select(sourceDf.columns.map(c => col(s"$SOURCE_ALIAS.$c")): _*)

    val newRecordsDf = sourceDf.join(targetCurrentDf, naturalKeyColumns, "left_anti")

    var mergeSourceDf = changedSourceDf.withColumn("_merge_action", lit("UPDATE"))
      .unionByName(changedSourceDf.withColumn("_merge_action", lit("INSERT")))
      .unionByName(newRecordsDf.withColumn("_merge_action", lit("INSERT")))

    if (isSourceSnapshot) {
      val expiredRecordsDf = targetCurrentDf.join(sourceDf, naturalKeyColumns, "left_anti")
      mergeSourceDf = mergeSourceDf.unionByName(expiredRecordsDf.withColumn("_merge_action", lit("UPDATE")))
    }

    val condition = getMergeCondition(naturalKeyColumns)
    val mergeCondition = s"""
                       | $condition
                       | AND $TARGET_ALIAS.$IS_CURRENT_COL = true
                       | AND $SOURCE_ALIAS._merge_action = 'UPDATE'
       """.stripMargin

    target.as(TARGET_ALIAS)
      .merge(mergeSourceDf.as(SOURCE_ALIAS), mergeCondition)
      .whenMatched
      .updateExpr(
        Map(
          VALID_TO_COL -> s"to_date('$ingestDate')",
          IS_CURRENT_COL -> "false"
        )
      )
      .whenNotMatched
      .insertExpr(
        sourceDf.columns.map(c => c -> s"$SOURCE_ALIAS.$c").toMap ++
          Map(
            VALID_FROM_COL -> s"to_date('$ingestDate')",
            VALID_TO_COL -> "NULL",
            IS_CURRENT_COL -> "true"
          )
      )
      .execute
  }

}
