package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import com.github.thomashan.spark.common.LoadCsvFileTask
import com.github.thomashan.spark.hilow.diff.DifferentiateTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteDatasetTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val inputFile = taskParameters("inputFile").toString
    val outputFile = taskParameters("outputFile").toString
    val header = taskParameters("header").asInstanceOf[Boolean]
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString

    def createTaskParameters(input: DataFrame): Map[String, Any] = {
      Map(
        "input" -> input,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      )
    }

    val input = new LoadCsvFileTask()
      .run(Map(
        "inputFile" -> inputFile,
        "header" -> header
      )).get.cache

    val diff = new DifferentiateTask()
      .run(createTaskParameters(input))
      .get

    val candidateExtremaSet = new CandidateExtremaSetTask()
      .run(createTaskParameters(diff))
      .get
    val candidateExtremaSetDedup = candidateExtremaSet
      .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)

    candidateExtremaSetDedup.count

//    val removeUnusedExtremasTask = new RemoveUnusedExtremasTask()
//    val extremaSet = removeUnusedExtremasTask
//      .run(Map(
//        "extremas" -> candidateExtremaSet,
//        "extremas_deduped" -> candidateExtremaSetDedup,
//        "xAxisName" -> xAxisName,
//        "hiSeriesName" -> hiSeriesName,
//        "lowSeriesName" -> lowSeriesName
//      )).get
//
//    val completeDataset = input
//      .join(extremaSet, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")
//      .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
//
//    completeDataset
//      .coalesce(1)
//      .orderBy(xAxisName)
//      .write
//      .option("header", true)
//      .mode("overwrite")
//      .csv(outputFile)
//
//    removeUnusedExtremasTask.caches.map(cache => cache.unpersist)

    Some(candidateExtremaSetDedup)
  }
}
