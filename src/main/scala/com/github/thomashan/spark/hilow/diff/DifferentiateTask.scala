package com.github.thomashan.spark.hilow.diff

import com.github.thomashan.spark.SparkTask
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class DifferentiateTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    import spark.implicits._

    val input = taskParameters("input").asInstanceOf[DataFrame]
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    Some(input
      .select(col(xAxisName), col(hiSeriesName), col(lowSeriesName))
      .orderBy(xAxisName)
      .rdd
      .sliding(2)
      .map { array =>
        val element0 = array.head
        val element1 = array.last
        val x0 = element0.getDouble(0)
        val x1 = element1.getDouble(0)
        val hiSeries0 = element0.getDouble(1)
        val hiSeries1 = element1.getDouble(1)
        val lowSeries0 = element0.getDouble(2)
        val lowSeries1 = element1.getDouble(2)

        val hiSeriesDiff = (hiSeries1 - hiSeries0) / (x1 - x0)
        val lowSeriesDiff = (lowSeries1 - lowSeries0) / (x1 - x0)

        (x1, hiSeries1, lowSeries1, hiSeriesDiff, lowSeriesDiff)
      }
      .toDF(xAxisName, hiSeriesName, lowSeriesName, "diff_" + hiSeriesName, "diff_" + lowSeriesName)
    )
  }
}
