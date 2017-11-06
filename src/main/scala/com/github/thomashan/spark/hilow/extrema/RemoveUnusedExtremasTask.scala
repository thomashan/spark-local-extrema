package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class RemoveUnusedExtremasTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val extremas = taskParameters("extremas").asInstanceOf[DataFrame].cache
    val extremasDeduped = taskParameters("extremas_deduped").asInstanceOf[DataFrame].cache
    addToCache(extremas, extremasDeduped)
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    val extremaSet = extremasDeduped
      .removeUnusedExtremas(xAxisName, hiSeriesName, lowSeriesName)
    val extremaSetDeduped = extremaSet
      .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)

    println("extremaSet.count: " + extremaSet.count)
    println("extremaSetDeduped.count: " + extremaSetDeduped.count)

    if (extremas.count != extremasDeduped.count) {
      run(Map(
        "extremas" -> extremaSet,
        "extremas_deduped" -> extremaSetDeduped,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      ))
    } else {
      Some(extremaSet)
    }
  }
}
