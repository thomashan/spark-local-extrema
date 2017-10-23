package com.github.thomashan.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkTask {
  implicit val spark: SparkSession
  var caches: Option[Seq[DataFrame]] = None

  def run(taskParameters: Map[String, Any] = Map()): Option[DataFrame]
}
