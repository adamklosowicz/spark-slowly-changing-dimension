package io.github.adamklosowicz.scd

import io.github.adamklosowicz.scd.exceptions.{DeltaRequiredException, IncompatibleSchemasException}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Helper {

  def getCondition(
    columns: Seq[String],
    comparisonOperator: String = "=",
    logicalOperator: String = "AND",
    conditionTemplate: String = "<condition>",
    leftAlias: String = "target",
    rightAlias: String = "source"
  ): String =
    columns.map(k => conditionTemplate.replace("<condition>", s"$leftAlias.$k $comparisonOperator $rightAlias.$k")).mkString(s" $logicalOperator ")

  def getTrackedColumns(df: DataFrame, columnsToExclude: Seq[String]): Seq[String] =
    df.columns.filterNot(columnsToExclude.contains(_))

  def pathExists(path: String)(implicit spark: SparkSession): Boolean = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(p)
  }

  def validateSchema(targetDf: DataFrame, sourceDf: DataFrame, scdSchema: StructType): Unit = {
    val actualFields = (sourceDf.schema.fields ++ scdSchema).map(f => f.name -> f).toMap
    val expectedFields = targetDf.schema.fields.map(f => f.name -> f).toMap

    val missing = expectedFields.keySet.diff(actualFields.keySet).map(name => s"Missing column: $name")

    val unexpected = actualFields.keySet.diff(expectedFields.keySet).map(name => s"Unexpected column: $name")

    val wrongTypes = expectedFields.keySet
      .intersect(actualFields.keySet)
      .flatMap { name =>
        val expectedField = expectedFields(name)
        val actualField = actualFields(name)

        if (expectedField.dataType != actualField.dataType) {
          Some(s"Column '$name': expected ${expectedField.dataType}, actual ${actualField.dataType}")
        } else {
          None
        }
      }

    val issues = (missing ++ unexpected ++ wrongTypes).toSeq
    if (issues.nonEmpty) {
      throw new IncompatibleSchemasException(issues)
    }
  }

  def validateDelta(path: String)(implicit spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      throw new DeltaRequiredException(path)
    }
  }

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
