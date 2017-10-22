package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import com.github.thomashan.spark.cartesian.diff.DifferentiateTask
import com.github.thomashan.spark.common.LoadCsvFileTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteDatasetTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val inputFile = taskParameters("inputFile").toString
    val outputFile = taskParameters("outputFile").toString
    val header = taskParameters("header").asInstanceOf[Boolean]
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString

    val input = new LoadCsvFileTask()
      .run(Map(
        "inputFile" -> inputFile,
        "header" -> header
      )).get.cache

    val diff = new DifferentiateTask()
      .run(Map(
        "input" -> input,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      )).get.cache

    val extremaSet = new ExtremaSetTask()
      .run(Map(
        "input" -> diff,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      )).get

    val result = input
      .join(extremaSet, Seq(xAxisName, yAxisName), "left")
      .select(xAxisName, yAxisName, "extrema", "extrema_index")

    result
      .coalesce(1)
      .orderBy(xAxisName)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputFile)

    input.unpersist
    diff.unpersist

    Some(result)
  }
}
