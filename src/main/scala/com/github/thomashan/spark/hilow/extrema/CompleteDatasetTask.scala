package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkTask
import com.github.thomashan.spark.common.LoadCsvFileTask
import com.github.thomashan.spark.hilow.diff.DifferentiateTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteDatasetTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val inputFile = taskParameters("inputFile").toString
    val header = taskParameters("header").asInstanceOf[Boolean]
    val xAxisName = taskParameters("xAxisName").toString
    val hiSeriesName = taskParameters("hiSeriesName").toString
    val lowSeriesName = taskParameters("lowSeriesName").toString
    val minimumDistance = taskParameters.getOrElse("minimumDistance", 0.0).asInstanceOf[Double]

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
      )).get

    val diff = new DifferentiateTask()
      .run(createTaskParameters(input))
      .get

    val candidateExtremaSet = new CandidateExtremaSetTask()
      .run(createTaskParameters(diff))
      .get
      .cache

    val candidateExtremaSetDedup = candidateExtremaSet
      .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)

    val extremaSet = new RemoveUnusedExtremaTask(minimumDistance).run(Map(
      "extrema_deduped" -> candidateExtremaSetDedup,
      "xAxisName" -> xAxisName,
      "hiSeriesName" -> hiSeriesName,
      "lowSeriesName" -> lowSeriesName
    )).get

    val completeDataset = input
      .join(extremaSet, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")

    Some(completeDataset)
  }
}
