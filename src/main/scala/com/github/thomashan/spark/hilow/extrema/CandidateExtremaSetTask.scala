package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CandidateExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val diff = taskParameters("input").asInstanceOf[DataFrame]
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    Some(
      diff
        .findCandidateExtremas(xAxisName, hiSeriesName, lowSeriesName)
        .orderBy(xAxisName)
    )
  }
}
