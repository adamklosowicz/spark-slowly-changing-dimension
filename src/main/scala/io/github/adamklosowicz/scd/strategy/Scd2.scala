package io.github.adamklosowicz.scd.strategy

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Scd2 extends ScdStrategy {

  override def apply(
    sourceDf: DataFrame,
    targetPath: String,
    businessKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(spark, targetPath)) {
      saveInitVersion(sourceDf, targetPath,ingestDate)
    } else {
      merge(sourceDf, targetPath, businessKeyColumns, ingestDate, technicalColumns)
    }
  }

  protected def saveInitVersion(sourceDf: DataFrame, targetPath: String, ingestDate: String): Unit = {
    sourceDf
      .withColumn("effective_from_date", lit(ingestDate).cast(DateType))
      .withColumn("effective_to_date", lit(null).cast(DateType))
      .withColumn("is_current", lit(true))
      .write.format("delta").mode("overwrite")
      .save(targetPath)
  }

  protected def merge(
    sourceDf: DataFrame,
    targetPath: String,
    businessKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit = {
    val target = DeltaTable.forPath(spark, targetPath)
    val targetCurrentDf = target.toDF.where(col("is_current") === true)

    val columnsToExclude = Seq(
      businessKeyColumns,
      technicalColumns,
      Seq("effective_from_date", "effective_to_date", "is_current")
    )
    val trackedColumns = sourceDf.columns.filterNot(columnsToExclude.flatten.contains(_))

    val changedSource = sourceDf.alias("source")
      .join(targetCurrentDf.alias("target"), businessKeyColumns)
      .filter(trackedColumns.map(c => !col(s"source.$c").eqNullSafe(col(s"target.$c"))).reduce(_ || _))
      .select(sourceDf.columns.map(c => col(s"source.$c")): _*)

    val newRecords = sourceDf.alias("source")
      .join(targetCurrentDf.alias("target"), businessKeyColumns, "left_anti")

    val mergeSource = changedSource.withColumn("_merge_action", lit("UPDATE"))
      .unionByName(changedSource.withColumn("_merge_action", lit("INSERT")))
      .unionByName(newRecords.withColumn("_merge_action", lit("INSERT")))

    val mergeCondition = businessKeyColumns.map(k => s"target.$k = source.$k").mkString(" AND ")
    val condition = s"""
                       | $mergeCondition
                       | AND target.is_current = true
                       | AND source._merge_action = 'UPDATE'
       """.stripMargin

    target.as("target")
      .merge(mergeSource.as("source"), condition)
      .whenMatched
      .updateExpr(
        Map(
          "effective_to_date" -> s"to_date('$ingestDate')",
          "is_current" -> "false"
        )
      )
      .whenNotMatched
      .insertExpr(
        sourceDf.columns.map(c => c -> s"source.$c").toMap ++
          Map(
            "effective_from_date" -> s"to_date('$ingestDate')",
            "effective_to_date" -> "NULL",
            "is_current" -> "true"
          )
      )
      .execute()
  }

}
