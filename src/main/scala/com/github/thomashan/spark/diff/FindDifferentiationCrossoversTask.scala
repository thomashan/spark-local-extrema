package com.github.thomashan.spark.diff

import com.github.thomashan.spark.SparkTask
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class FindDifferentiationCrossoversTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    import spark.implicits._

    val input = taskParameters("input").asInstanceOf[DataFrame]
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString

    Some(input
      .select(col(xAxisName), col(yAxisName), $"diff")
      .orderBy(xAxisName)
      .rdd
      .sliding(2)
      .map { array =>
        val element0 = array.head
        val element1 = array.last
        val x0 = element0.getDouble(0)
        val y0 = element0.getDouble(1)
        val diff0 = element0.getDouble(2)
        val diff1 = element1.getDouble(2)

        val extrema = if (diff0 > 0 && diff1 < 0) {
          "maxima"
        } else if (diff0 < 0 && diff1 > 0) {
          "minima"
        } else {
          null
        }

        (x0, y0, diff0, extrema)
      }
      .toDF(xAxisName, yAxisName, "diff", "extrema"))
  }
}
