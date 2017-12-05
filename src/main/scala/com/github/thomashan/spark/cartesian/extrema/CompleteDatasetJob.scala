package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkJob

// docker run --rm -it -p 4040:4040 \
// -v $(pwd)/examples:/data \
// -v $(pwd)/target/scala-2.11/spark-local-extrema-assembly-0.1-SNAPSHOT.jar:/job.jar \
// gettyimages/spark:2.2.0-hadoop-2.7 bin/spark-submit \
// --master local[*] \
// --driver-memory 2g \
// --class com.github.thomashan.spark.cartesian.extrema.CompleteDatasetJob /job.jar \
// /data/random.csv true x y /data/output/random_extrema
class CompleteDatasetJob extends SparkJob {
  override val applicationName: String = getClass.getName

  override protected def run(args: Array[String]): Unit = {
    val inputFile = args(0)
    val header = args(1).toBoolean
    val xAxisName = args(2)
    val yAxisName = args(3)
    val outputFile = args(4)

    new CompleteDatasetTask().run(Map(
      "inputFile" -> inputFile,
      "header" -> header,
      "xAxisName" -> xAxisName,
      "yAxisName" -> yAxisName,
      "outputFile" -> outputFile
    ))
  }
}

object CompleteDatasetJob extends App {
  new CompleteDatasetJob().run(args)
}
