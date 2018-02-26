package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkJob

// docker run --rm -it -p 4040:4040 \
// -v $(pwd)/examples:/data \
// -v $(pwd)/target/scala-2.11/spark-local-extrema-assembly-0.1-SNAPSHOT.jar:/job.jar \
// gettyimages/spark:2.2.0-hadoop-2.7 bin/spark-submit \
// --master local[*] \
// --driver-memory 2g \
// --class com.github.thomashan.spark.hilow.extrema.CompleteDatasetJob /job.jar \
// /data/hi_low.csv true x hi low /data/output/hi_low_extrema
class CompleteDatasetJob extends SparkJob {
  override val applicationName: String = getClass.getName

  override protected def run(args: Array[String]): Unit = {
    val inputFile = args(0)
    val header = args(1).toBoolean
    val xAxisName = args(2)
    val hiSeriesName = args(3)
    val lowSeriesName = args(4)
    val outputFile = args(5)

    val completeDataset = new CompleteDatasetTask().run(Map(
      "inputFile" -> inputFile,
      "header" -> header,
      "xAxisName" -> xAxisName,
      "hiSeriesName" -> hiSeriesName,
      "lowSeriesName" -> lowSeriesName,
      "outputFile" -> outputFile
    )).get

    completeDataset
      .coalesce(1)
      .orderBy(xAxisName)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputFile)
  }
}

object CompleteDatasetJob extends App {
  new CompleteDatasetJob().run(args)
}
