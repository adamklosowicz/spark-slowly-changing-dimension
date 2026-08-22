package io.github.adamklosowicz.scd.strategies

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ScdStrategy {

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    naturalKeyColumns: Seq[String],
    ingestDate: String,
    isSourceSnapshot: Boolean = false,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit

}
