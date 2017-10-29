package com.github.thomashan.spark.hilow

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object extrema {

  implicit class Methods(dataFrame: DataFrame) {

    import dataFrame.sqlContext.implicits._

    def findCrossovers(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val hiSeriesDiff = "diff_" + hiSeriesName
      val lowSeriesDiff = "diff_" + lowSeriesName

      dataFrame
        .select(col(xAxisName), col(hiSeriesName), col(lowSeriesName), col(hiSeriesDiff), col(lowSeriesDiff))
        .where(col(hiSeriesDiff) =!= 0 || col(lowSeriesDiff) =!= 0)
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last
          val x0 = element0.getDouble(0)
          val hi0 = element0.getDouble(1)
          val low0 = element0.getDouble(2)
          val hiDiff0 = element0.getDouble(3)
          val hiDiff1 = element1.getDouble(3)
          val lowDiff0 = element0.getDouble(4)
          val lowDiff1 = element1.getDouble(4)

          val extrema = if (lowDiff0 > 0 && hiDiff1 < 0) {
            "maxima"
          } else if (hiDiff0 < 0 && lowDiff1 > 0) {
            "minima"
          } else {
            null
          }

          (x0, hi0, low0, hiDiff0, lowDiff0, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff, "extrema")
        .where($"extrema".isNotNull)
    }
  }

}
