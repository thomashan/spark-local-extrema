package com.github.thomashan.spark.diff

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class FindDifferentiationCrossoversTask(implicit val spark: SparkSession) extends SparkTask {

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString
    val input = taskParameters("input").asInstanceOf[DataFrame]
      .select(col(xAxisName), col(yAxisName))
      .orderBy(xAxisName)
      .cache

    val diff = input
      .diff(xAxisName, yAxisName)
      .cache

    val reducedExtremaSet = diff
      .filterOutZeroGradient
      .findCrossovers(xAxisName, yAxisName)
      .crossoverIndex(xAxisName, yAxisName)

    val extremaSet = diff
      .join(reducedExtremaSet, Seq(xAxisName, yAxisName, "diff"), "left")
      .allExtremas(xAxisName, yAxisName)

    val result = input
      .join(extremaSet, Seq(xAxisName, yAxisName), "left")
      .orderBy(xAxisName)
      .select(xAxisName, yAxisName, "extrema", "extrema_index")

    input.unpersist
    diff.unpersist

    Some(
      result
    )
  }
}
