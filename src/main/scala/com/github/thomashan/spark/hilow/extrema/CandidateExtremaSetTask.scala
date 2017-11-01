package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CandidateExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {

  import spark.implicits._

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val diff = taskParameters("input").asInstanceOf[DataFrame]
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString
    val hiSeriesDiff = "diff_" + hiSeriesName
    val lowSeriesDiff = "diff_" + lowSeriesName

    Some(
      diff
        .findCandidateExtremas(xAxisName, hiSeriesName, lowSeriesName)
        .rdd
        .zipWithIndex()
        .map { tuple =>
          val x = tuple._1.getDouble(0)
          val y = tuple._1.getDouble(1)
          val diff = tuple._1.getDouble(2)
          val extrema = tuple._1.getString(3)
          val index = tuple._2

          (x, y, diff, extrema, index)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff, "extrema", "extrema_index")
        .orderBy(xAxisName)
    )
  }
}
