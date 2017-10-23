package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReducedExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString
    val diff = taskParameters("input").asInstanceOf[DataFrame]

    Some(
      diff
        .filterOutZeroGradient
        .findCrossovers(xAxisName, yAxisName)
        .crossoverIndex(xAxisName, yAxisName)
    )
  }
}
