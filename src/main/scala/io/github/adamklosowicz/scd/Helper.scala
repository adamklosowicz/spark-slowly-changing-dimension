package io.github.adamklosowicz.scd

import org.apache.spark.sql.{Column, DataFrame}

object Helper {

  def getKeyMergeCondition(naturalKeyColumns: Seq[String], leftAlias: String = "target", rightAlias: String = "source"): String =
    naturalKeyColumns.map(k => s"$leftAlias.$k = $rightAlias.$k").mkString(" AND ")

  def getTrackedColumns(df: DataFrame, columnsToExclude: Seq[String]): Seq[String] =
    df.columns.filterNot(columnsToExclude.contains(_))

  implicit class DfExtender(df: DataFrame) {

    def applyColumns(columns: Map[String, Column]): DataFrame = {
      columns.foldLeft(df) { case (tmpDf, (columnName, rule)) =>
        tmpDf.withColumn(columnName, rule)
      }
    }

    def saveWithColumns(targetPath: String, columnsToApply: Map[String, Column] = Map(), mode: String = "overwrite"): Unit = {
      df.applyColumns(columnsToApply)
        .write.format("delta").mode(mode)
        .save(targetPath)
    }

  }

}
