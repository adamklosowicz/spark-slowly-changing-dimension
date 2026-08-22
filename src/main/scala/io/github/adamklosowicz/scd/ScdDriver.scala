package io.github.adamklosowicz.scd

import io.github.adamklosowicz.scd.exceptions.InvalidScdTypeException
import io.github.adamklosowicz.scd.strategy._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScdDriver {
  def apply(
    scdType: String,
    sourceDf: DataFrame,
    targetPath: String,
    businessKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit =
    scdType match {
      case "scd2" => Scd2(sourceDf, targetPath, businessKeyColumns, ingestDate, technicalColumns)
      case _ => throw new InvalidScdTypeException(scdType)
    }
}
