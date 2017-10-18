package com.github.thomashan.spark.diff

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class DifferentiateTask(implicit spark: SparkSession) {

  import spark.implicits._

  def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val input = taskParameters("input").asInstanceOf[DataFrame]
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString

    Some(input
      .select(col(xAxisName), col(yAxisName))
      .orderBy(xAxisName)
      .rdd
      .sliding(2)
      .map { array =>
        val element0 = array.head
        val element1 = array.last
        val y1 = element1.getDouble(1)
        val y0 = element0.getDouble(1)
        val x1 = element1.getDouble(0)
        val x0 = element0.getDouble(0)

        val diff = (y1 - y0) / (x1 - x0)

        (x1, y1, diff)
      }
      .toDF(xAxisName, yAxisName, "diff")
    )
  }
}
