package com.github.thomashan.spark.common

import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadCsvFileTask(implicit spark: SparkSession) {
  def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val header = taskParameters("header").asInstanceOf[Boolean]
    val inputFile = taskParameters("inputFile").toString

    Some(spark.read.option("header", header).csv(inputFile))
  }
}
