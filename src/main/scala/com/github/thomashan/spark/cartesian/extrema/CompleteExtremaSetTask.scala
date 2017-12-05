package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.SparkTask
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{currentRow, unboundedPreceding}
import org.apache.spark.sql.functions.{col, last, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompleteExtremaSetTask(implicit val spark: SparkSession) extends SparkTask {

  import spark.implicits._

  override def run(taskParameters: Map[String, Any]): Option[DataFrame] = {
    val xAxisName = taskParameters("xAxisName").toString
    val yAxisName = taskParameters("yAxisName").toString
    val diff = taskParameters("diff").asInstanceOf[DataFrame]
    val reducedExtremaSet = taskParameters("reducedExtremaSet").asInstanceOf[DataFrame]

    val startOfFlats = diff
      .join(reducedExtremaSet, Seq(xAxisName, yAxisName, "diff"), "left")
      .withColumn("null_out_x", when($"diff" === 0, null).otherwise(col(xAxisName)))
      .withColumn("start_of_flat_x", last("null_out_x", true).over(Window.orderBy(xAxisName).rowsBetween(unboundedPreceding, currentRow)))
      .cache

    addToCache(startOfFlats)

    Some(
      startOfFlats
        .allExtrema(xAxisName, yAxisName)
    )
  }
}
