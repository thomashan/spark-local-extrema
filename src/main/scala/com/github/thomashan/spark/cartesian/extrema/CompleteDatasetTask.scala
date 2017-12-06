package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import com.github.thomashan.spark.cartesian.diff.DifferentiateTask
import com.github.thomashan.spark.common.LoadCsvFileTask
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteDatasetTask(implicit val spark: SparkSession) extends SparkTask {
  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val inputFile = taskParameters("inputFile").toString
    val outputFile = taskParameters("outputFile").toString
    val header = taskParameters("header").asInstanceOf[Boolean]
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString

    val inputOption = new LoadCsvFileTask()
      .run(Map(
        "inputFile" -> inputFile,
        "header" -> header
      ))
    val input = inputOption match {
      case Some(input) => input.cache
      case _ => throw new RuntimeException(s"expected input to be present for ${inputFile}")
    }

    val diffOption = new DifferentiateTask()
      .run(Map(
        "input" -> input,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      ))

    val diff = diffOption match {
      case Some(diff) => diff.cache
      case _ => throw new RuntimeException(s"expected diff to be present for ${inputFile}")
    }

    val reducedExtremaSetOption = new ReducedExtremaSetTask()
      .run(Map(
        "input" -> diff,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      ))
    val reducedExtremaSet = reducedExtremaSetOption match {
      case Some(reducedExtremaSet) => reducedExtremaSet
      case _ => throw new RuntimeException(s"expected reducedExtremaSet to be present for ${inputFile}")
    }

    val completeExtremaSetTask = new CompleteExtremaSetTask()
    val extremaSetOption = completeExtremaSetTask
      .run(Map(
        "diff" -> diff,
        "reducedExtremaSet" -> reducedExtremaSet,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      ))
    val extremaSet = extremaSetOption match {
      case Some(extremaSet) => extremaSet
      case _ => throw new RuntimeException(s"expected extremaSet to be present for ${inputFile}")
    }

    val completeDataset = input
      .join(extremaSet, Seq(xAxisName, yAxisName), "left")
      .select(xAxisName, yAxisName, "extrema", "extrema_index")

    completeDataset
      .coalesce(1)
      .orderBy(xAxisName)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputFile)

    input.unpersist
    diff.unpersist

    Some(completeDataset)
  }
}
