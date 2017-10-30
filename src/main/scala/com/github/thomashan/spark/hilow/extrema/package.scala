package com.github.thomashan.spark.hilow

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object extrema {

  implicit private[hilow] class Methods(dataFrame: DataFrame) {

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

          val extrema = if (lowDiff0 > 0 && lowDiff1 < 0) {
            "maxima"
          } else if (hiDiff0 < 0 && hiDiff1 > 0) {
            "minima"
          } else {
            null
          }

          (x0, hi0, low0, hiDiff0, lowDiff0, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff, "extrema")
        .where($"extrema".isNotNull)
    }

    def removeDuplicateExtremas(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val hiSeriesDiff = "diff_" + hiSeriesName
      val lowSeriesDiff = "diff_" + lowSeriesName

      // FIXME: cache dataFrame!
      val firstElement = dataFrame.orderBy(xAxisName).limit(1)
      val lastElement = dataFrame.orderBy(col(xAxisName).desc).limit(1)

      dataFrame
        .orderBy("x")
        .select(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff, "extrema")
        .rdd
        .sliding(3)
        .map { array =>
          val element0 = array.head
          val element1 = array(1)
          val element2 = array.last
          val x = element1.getDouble(0)

          val previousHi = element0.getDouble(1)
          val previousLow = element0.getDouble(2)
          val currentHi = element1.getDouble(1)
          val currentLow = element1.getDouble(2)
          val nextHi = element2.getDouble(1)
          val nextLow = element2.getDouble(2)

          val diffHi = element1.getDouble(3)
          val diffLow = element1.getDouble(4)
          val oldExtrema = element1.getString(5)
          val previousExtrema = element0.getString(5)
          val nextExtrema = element2.getString(5)

          def extremasDifferent(previousExtrema: String, currentExtrema: String, nextExtrema: String): Boolean = {
            if (currentExtrema == previousExtrema || currentExtrema == nextExtrema) false else true
          }

          def isMaxima(currentExtrema: String) = {
            if (currentExtrema == "maxima") true else false
          }

          def getExtremaValue_Maxima(previousExtrema: String, currentExtrema: String)(previousLow: Double, currentLow: Double, nextLow: Double): String = {
            if (previousExtrema == currentExtrema) {
              if (previousLow > currentLow) null else currentExtrema
            } else {
              if (currentLow < nextLow) null else currentExtrema
            }
          }

          def getExtremaValue_Minima(previousExtrema: String, currentExtrema: String)(previousLow: Double, currentLow: Double, nextLow: Double): String = {
            if (previousExtrema == currentExtrema) {
              if (previousHi > currentHi) null else currentExtrema
            } else {
              if (currentHi < nextHi) null else currentExtrema
            }
          }

          val newExtrema = if (extremasDifferent(previousExtrema, oldExtrema, nextExtrema)) {
            oldExtrema
          } else {
            if (isMaxima(oldExtrema)) {
              getExtremaValue_Maxima(previousExtrema, oldExtrema)(previousLow, currentLow, nextLow)
            } else {
              getExtremaValue_Minima(previousExtrema, oldExtrema)(previousHi, currentHi, nextHi)
            }
          }

          (x, currentHi, currentLow, diffHi, diffLow, newExtrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff, "extrema")
        .where($"extrema".isNotNull)
        .union(firstElement)
        .union(lastElement)
        .orderBy(xAxisName)
    }
  }

}
