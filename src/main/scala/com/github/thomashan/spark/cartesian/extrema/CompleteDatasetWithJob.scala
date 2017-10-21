package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkJob
import com.github.thomashan.spark.cartesian.diff.DifferentiateTask
import com.github.thomashan.spark.common.LoadCsvFileTask

// spark-submit --master local[*] --driver-memory 4g \
// --class com.github.thomashan.spark.cartesian.extrema.CompleteDatasetWithJob \
// target/scala-2.11/spark-jobs-assembly-0.1-SNAPSHOT.jar inputFile xAxisName yAxisName outputFile
class CompleteDatasetWithJob extends SparkJob {
  override val applicationName: String = getClass.getName

  override protected def run(args: Array[String]): Unit = {
    val inputFile = args(0)
    val xAxisName = args(1)
    val yAxisName = args(2)
    val outputFile = args(3)

    val input = new LoadCsvFileTask()
      .run(Map(
        "inputFile" -> inputFile
      )).get.cache

    val diff = new DifferentiateTask()
      .run(Map(
        "input" -> input,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      )).get.cache

    val extremaSet = new ExtremaSetTask()
      .run(Map(
        "input" -> diff,
        "xAxisName" -> xAxisName,
        "yAxisName" -> yAxisName
      )).get

    val result = input
      .join(extremaSet, Seq(xAxisName, yAxisName), "left")
      .orderBy(xAxisName)
      .select(xAxisName, yAxisName, "extrema", "extrema_index")

    result
      .write
      .mode("overwrite")
      .save(outputFile)
  }
}

