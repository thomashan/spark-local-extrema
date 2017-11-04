package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class RemoveUnusedExtremasTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val extremas = taskParameters("extremas").asInstanceOf[DataFrame]
    val extremasDeduped = taskParameters("extremas_deduped").asInstanceOf[DataFrame].cache
    addToCache(extremas, extremasDeduped)
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    val extremaSet = extremasDeduped
      .removeUnusedExtremas(xAxisName, hiSeriesName, lowSeriesName)
    val extremaSetDeduped = extremaSet
      .removeUnusedExtremas(xAxisName, hiSeriesName, lowSeriesName)

    if (extremas.count != extremasDeduped.count) {
      return run(Map(
        "extremas" -> extremaSet,
        "extremas_deduped" -> extremaSetDeduped,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      ))
    }

    Some(extremaSet)
  }
}
