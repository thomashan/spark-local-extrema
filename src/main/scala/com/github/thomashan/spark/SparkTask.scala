package com.github.thomashan.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkTask {
  implicit val spark: SparkSession
  var caches: Seq[DataFrame] = Seq()

  def run(taskParameters: Map[String, Any] = Map()): Option[DataFrame]

  def addToCache(dataFrames: DataFrame*) = {
    dataFrames.foreach(dataFrame => caches = caches :+ dataFrame)
  }
}
