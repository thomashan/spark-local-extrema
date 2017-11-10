package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class RemoveUnusedExtremasTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val extremasDeduped = taskParameters("extremas_deduped").asInstanceOf[DataFrame].cache
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    val extremaSet = extremasDeduped
      .removeUnusedExtremas(xAxisName, hiSeriesName, lowSeriesName)
      .cache
    val extremaSetDeduped = extremaSet
      .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)
      .cache

    addToCache(extremaSet, extremaSetDeduped)

    println("extremaSet.count: " + extremaSet.count)
    println("extremaSetDeduped.count: " + extremaSetDeduped.count)

    if (extremasDeduped.count == extremaSet.count || extremaSet.count == extremaSetDeduped.count) {
      Some(extremaSetDeduped)
    } else {
      run(Map(
        "extremas_deduped" -> extremaSetDeduped,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      ))
    }
  }
}
