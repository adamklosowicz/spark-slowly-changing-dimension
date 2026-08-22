package io.github.adamklosowicz.scd

import io.github.adamklosowicz.scd.exceptions.InvalidScdTypeException
import io.github.adamklosowicz.scd.strategies._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScdDriver {

  def apply(
    scdType: String,
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: String,
    isSourceSnapshot: Boolean = false,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit =
    scdType.toLowerCase match {
      case "scd2" => Scd2(sourceDf, targetPath, naturalKeyColumns, ingestDate, isSourceSnapshot, technicalColumns)
      case _ => throw new InvalidScdTypeException(scdType)
    }

}
