package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object Scd1 {

  protected val CREATED_AT_COL = "created_at"
  protected val UPDATED_AT_COL = "updated_at"

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(spark, targetPath)) {
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
    val sourceDfToSave = if (includeDateCol) {
      sourceDf
        .withColumn(CREATED_AT_COL, lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType))
        .withColumn(UPDATED_AT_COL, lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType))
    } else {
      sourceDf
    }
    sourceDfToSave.write.format("delta").mode("overwrite")
      .save(targetPath)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    val target = DeltaTable.forPath(spark, targetPath)

    val mergeCondition = naturalKeyColumns.map(k => s"target.$k = source.$k").mkString(" AND ")

    val technicalMap1: Map[String, String] = if (includeDateCol) {
      val ingestDateStr = ingestDate.getOrElse(LocalDate.now())
      Map(UPDATED_AT_COL -> s"to_date('$ingestDateStr')")
    } else Map()
    val technicalMap2: Map[String, String] = if (includeDateCol) {
      technicalMap1 ++ Map(CREATED_AT_COL -> technicalMap1(UPDATED_AT_COL))
    } else Map()

    target.as("target")
      .merge(sourceDf.as("source"), mergeCondition)
      .whenMatched()
      .updateExpr(sourceDf.columns.map(c => c -> s"source.$c").toMap ++ technicalMap1)
      .whenNotMatched()
      .insertExpr(sourceDf.columns.map(c => c -> s"source.$c").toMap ++ technicalMap2)
      .execute()
  }

}

