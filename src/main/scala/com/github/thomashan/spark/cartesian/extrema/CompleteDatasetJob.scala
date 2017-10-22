package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkJob
import com.github.thomashan.spark.cartesian.diff.DifferentiateTask
import com.github.thomashan.spark.common.LoadCsvFileTask

// docker run --rm -it -p 4040:4040 \
// -v $(pwd)/examples:/data \
// -v $(pwd)/target/scala-2.11/spark-local-extrema-assembly-0.1-SNAPSHOT.jar:/job.jar \
// gettyimages/spark bin/spark-submit --master local[*] --driver-memory 2g --class com.github.thomashan.spark.cartesian.extrema.CompleteDatasetJob /job.jar /data/random.csv true x y /data/random_extremas
class CompleteDatasetJob extends SparkJob {
  override val applicationName: String = getClass.getName

  override protected def run(args: Array[String]): Unit = {
    val inputFile = args(0)
    val header = args(1).toBoolean
    val xAxisName = args(2)
    val yAxisName = args(3)
    val outputFile = args(4)

    val input = new LoadCsvFileTask()
      .run(Map(
        "inputFile" -> inputFile,
        "header" -> header
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
      .select(xAxisName, yAxisName, "extrema", "extrema_index")

    result
      .coalesce(1)
      .orderBy(xAxisName)
      .write
      .option("header", true)
      .mode("overwrite")
      .csv(outputFile)

    input.unpersist
    diff.unpersist
  }
}

object CompleteDatasetJob extends App {
  new CompleteDatasetJob().run(args)
}
