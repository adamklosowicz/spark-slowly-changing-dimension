package io.github.adamklosowicz.scd.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, concat_ws, sha2}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait ScdCommon {

  protected val TARGET_ALIAS = "target"
  protected val SOURCE_ALIAS = "source"

  protected def getMergeCondition(
    columns: Seq[String],
    comparisonOperator: String = "=",
    logicalOperator: String = "AND",
    conditionTemplate: String = "<condition>"
  ): String =
    columns.map(k => conditionTemplate.replace("<condition>", s"$TARGET_ALIAS.$k $comparisonOperator $SOURCE_ALIAS.$k")).mkString(s" $logicalOperator ")

  protected def getTrackedColumns(df: DataFrame, columnsToExclude: Seq[String]): Seq[String] =
    df.columns.filterNot(columnsToExclude.contains(_))

  protected def pathExists(path: String)(implicit spark: SparkSession): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(p)
  }

  implicit class DfExtender(df: DataFrame) {

    def withHashColumn(name: String, includeHashColumn: Boolean, columnsToHash: Seq[String]): DataFrame =
      if (includeHashColumn) {
        df.withColumn(name, sha2(concat_ws("|", columnsToHash.map(col): _*), 256))
      } else {
        df
      }

    def applyColumns(columns: Map[String, Column]): DataFrame =
      columns.foldLeft(df) { case (tmpDf, (columnName, rule)) =>
        tmpDf.withColumn(columnName, rule)
      }

    def saveWithColumns(targetPath: String, columnsToApply: Map[String, Column] = Map(), mode: String = "overwrite"): Unit =
      df.applyColumns(columnsToApply)
        .write.format("delta").mode(mode)
        .save(targetPath)

  }
}
