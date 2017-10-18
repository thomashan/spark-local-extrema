package com.github.thomashan.spark.common

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadCsvFileTask(implicit val spark: SparkSession) extends SparkTask {
  def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val header = taskParameters("header").asInstanceOf[Boolean]
    val inputFile = taskParameters("inputFile").toString

    Some(spark.read.option("header", header).option("inferSchema", true).csv(inputFile))
  }
}
