package io.github.adamklosowicz.scd.strategy

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ScdStrategy {

  def apply(
    sourceDf: DataFrame,
    targetPath: String,
    businessKeyColumns: Seq[String],
    ingestDate: String,
    technicalColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): Unit

}
