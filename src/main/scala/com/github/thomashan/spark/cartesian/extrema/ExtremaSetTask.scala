package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString
    val input = taskParameters("input").asInstanceOf[DataFrame]

    val reducedExtremaSet = input
      .filterOutZeroGradient
      .findCrossovers(xAxisName, yAxisName)
      .crossoverIndex(xAxisName, yAxisName)

    val extremaSet = input
      .join(reducedExtremaSet, Seq(xAxisName, yAxisName, "diff"), "left")
      .allExtremas(xAxisName, yAxisName)
      .select(xAxisName, yAxisName, "extrema", "extrema_index")
      .orderBy(xAxisName)

    Some(
      extremaSet
    )
  }
}
