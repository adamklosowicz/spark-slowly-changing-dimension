package io.github.adamklosowicz.scd.utils

import io.delta.tables.DeltaTable
import io.github.adamklosowicz.scd.exceptions.{DeltaRequiredException, IncompatibleSchemasException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ScdValidator {

  protected def validateSchema(targetDf: DataFrame, sourceDf: DataFrame, scdSchema: StructType): Unit = {
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

  protected def validateDelta(path: String)(implicit spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      throw new DeltaRequiredException(path)
    }
  }
}
