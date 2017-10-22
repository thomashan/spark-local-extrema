package com.github.thomashan.spark.cartesian

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{currentRow, unboundedPreceding}
import org.apache.spark.sql.functions._

package object extrema {

  implicit private[cartesian] class Methods(dataFrame: DataFrame) {

    import dataFrame.sqlContext.implicits._

    def filterOutZeroGradient(): DataFrame = {
      dataFrame
        .where($"diff" =!= 0)
    }

    def findCrossovers(xAxisName: String, yAxisName: String): DataFrame = {
      dataFrame
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last
          val x0 = element0.getDouble(0)
          val y0 = element0.getDouble(1)
          val diff0 = element0.getDouble(2)
          val diff1 = element1.getDouble(2)
          val extrema = if (diff0 > 0 && diff1 < 0) {
            "maxima"
          } else if (diff0 < 0 && diff1 > 0) {
            "minima"
          } else {
            null
          }

          (x0, y0, diff0, extrema)
        }
        .toDF(xAxisName, yAxisName, "diff", "extrema")
        .where($"extrema".isNotNull)
    }


    def crossoverIndex(xAxisName: String, yAxisName: String) = {
      dataFrame
        .where($"extrema".isNotNull)
        .rdd
        .zipWithIndex()
        .map { tuple =>
          val x = tuple._1.getDouble(0)
          val y = tuple._1.getDouble(1)
          val diff = tuple._1.getDouble(2)
          val extrema = tuple._1.getString(3)
          val index = tuple._2

          (x, y, diff, extrema, index)
        }
        .toDF(xAxisName, yAxisName, "diff", "extrema", "extrema_index")
    }

    def allExtremas(xAxisName: String, yAxisName: String): DataFrame = {
      val startOfFlats = dataFrame
        .withColumn("null_out_x", when($"diff" === 0, null).otherwise(col(xAxisName)))
        .withColumn("start_of_flat_x", last("null_out_x", true).over(Window.orderBy(xAxisName).rowsBetween(unboundedPreceding, currentRow)))

      val zeroDiffAreas = startOfFlats
        .groupBy("start_of_flat_x")
        .agg(
          first("extrema").as("zero_diff_extrema"),
          first("extrema_index").as("zero_diff_extrema_index"),
          count(col(xAxisName)).as("count")
        )
        .where($"count" > 1)


      // FIXME: should refactor as the startOfFlats needs to be cached
      startOfFlats
        .join(zeroDiffAreas, Seq("start_of_flat_x"), "left")
        .withColumn("temp_extrema", when($"diff" === 0, $"zero_diff_extrema").otherwise($"extrema"))
        .withColumn("temp_extrema_index", when($"diff" === 0, $"zero_diff_extrema_index").otherwise($"extrema_index"))
        .select(col(xAxisName), col(yAxisName), $"diff", $"temp_extrema".as("extrema"), $"temp_extrema_index".as("extrema_index"))
        .where($"extrema".isNotNull)
    }
  }

}
