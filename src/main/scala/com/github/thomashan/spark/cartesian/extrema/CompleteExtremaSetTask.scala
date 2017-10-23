package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString
    val diff = taskParameters("diff").asInstanceOf[DataFrame]
    val reducedExtremaSet = taskParameters("reducedExtremaSet").asInstanceOf[DataFrame]

    Some(
      diff
        .join(reducedExtremaSet, Seq(xAxisName, yAxisName, "diff"), "left")
        .allExtremas(xAxisName, yAxisName)
        .select(xAxisName, yAxisName, "extrema", "extrema_index")
        .orderBy(xAxisName)
    )
  }
}
