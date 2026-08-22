package io.github.adamklosowicz.scd.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object TestHelper {

  def removePath(path: String)(implicit spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(path), true)
  }

}
