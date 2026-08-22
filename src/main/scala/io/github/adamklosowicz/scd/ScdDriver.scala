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
    ingestDate: Option[String],
    isSourceSnapshot: Option[Boolean] = None,
    technicalColumns: Option[Seq[String]] = None
  )(implicit spark: SparkSession): Unit =
    scdType.toLowerCase match {
      case "scd0" => Scd0(sourceDf, targetPath, naturalKeyColumns, includeDateCol = true, ingestDate)
      case "scd1" => Scd1(sourceDf, targetPath, naturalKeyColumns, includeDateCol = true, ingestDate)
      case "scd2" => Scd2(sourceDf, targetPath, naturalKeyColumns, ingestDate, isSourceSnapshot.getOrElse(false), technicalColumns.getOrElse(Seq.empty))
      case _ => throw new InvalidScdTypeException(scdType)
    }

}
