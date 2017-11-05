package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkJob

class CompleteDatasetJob extends SparkJob {
  override val applicationName: String = getClass.getName

  override protected def run(args: Array[String]): Unit = {
    val inputFile = args(0)
    val header = args(1).toBoolean
    val xAxisName = args(2)
    val hiSeriesName = args(3)
    val lowSeriesName = args(4)
    val outputFile = args(5)

    new CompleteDatasetTask().run(Map(
      "inputFile" -> inputFile,
      "header" -> header,
      "xAxisName" -> xAxisName,
      "hiSeriesName" -> hiSeriesName,
      "lowSeriesName" -> lowSeriesName,
      "outputFile" -> outputFile
    ))
  }
}

object CompleteDatasetJob extends App {
  new CompleteDatasetJob().run(args)
}
