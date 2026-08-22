package io.github.adamklosowicz.scd.strategies

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object Scd0 {

  protected val CREATED_AT_COL = "created_at"

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeCreationDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(spark, targetPath)) {
      saveInitVersion(sourceDf, targetPath, includeCreationDateCol, ingestDate)
    } else {
      merge(sourceDf, targetPath, naturalKeyColumns, includeCreationDateCol, ingestDate)
    }
  }

  protected def saveInitVersion(
    sourceDf: DataFrame,
    targetPath: String,
    includeCreationDateCol: Boolean = false,
    ingestDate: Option[String] = None
  ): Unit = {
    val sourceToSaveDf = if (includeCreationDateCol) {
      sourceDf.withColumn(CREATED_AT_COL, lit(ingestDate.getOrElse(LocalDate.now())).cast(DateType))
    } else {
      sourceDf
    }
    sourceToSaveDf.write.format("delta").mode("overwrite")
      .save(targetPath)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    includeCreationDateCol: Boolean = false,
    ingestDate: Option[String] = None
  )(implicit spark: SparkSession): Unit = {
    val target = DeltaTable.forPath(spark, targetPath)

    val mergeCondition = naturalKeyColumns.map(k => s"target.$k = source.$k").mkString(" AND ")
    val technicalMap = if (includeCreationDateCol) {
      val ingestDateStr = ingestDate.getOrElse(LocalDate.now())
      Map(CREATED_AT_COL -> s"to_date('$ingestDateStr')")
    } else Map()

    target.as("target")
      .merge(sourceDf.as("source"), mergeCondition)
      .whenNotMatched()
      .insertExpr(sourceDf.columns.map(c => c -> s"source.$c").toMap ++ technicalMap)
      .execute()
  }

}

