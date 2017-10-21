package com.github.thomashan.spark.cartesian

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{currentRow, unboundedFollowing, unboundedPreceding}
import org.apache.spark.sql.functions.{col, first, last}

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
      dataFrame
        .withColumn("last_extrema", last("extrema", true).over(Window.orderBy(xAxisName).rowsBetween(unboundedPreceding, currentRow)))
        .withColumn("first_extrema", first("extrema", true).over(Window.orderBy(xAxisName).rowsBetween(currentRow, unboundedFollowing)))
        .where($"first_extrema".isNotNull && $"last_extrema".isNotNull)
        .where($"extrema".isNotNull || $"diff" === 0)
        .withColumn("last_extrema_index", last("extrema_index", true).over(Window.orderBy(xAxisName).rowsBetween(unboundedPreceding, currentRow)))
        .select(col(xAxisName), col(yAxisName), $"diff", $"last_extrema".as("extrema"), $"last_extrema_index".as("extrema_index"))
    }
  }

}
