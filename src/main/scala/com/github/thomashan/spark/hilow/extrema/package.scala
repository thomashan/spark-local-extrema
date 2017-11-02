package com.github.thomashan.spark.hilow

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object extrema {

  implicit private[hilow] class Methods(dataFrame: DataFrame) {

    import dataFrame.sqlContext.implicits._

    def join(xAxisName: String, extremas: DataFrame): DataFrame = {
      dataFrame
        .join(extremas, Seq(xAxisName), "left")
        .orderBy(xAxisName)
    }

    def findCandidateExtremas(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      // FIXME: pull out logic for constructing hi series diff
      val hiSeriesDiff = "diff_" + hiSeriesName
      val lowSeriesDiff = "diff_" + lowSeriesName

      dataFrame
        // FIXME: get rid of unused columns
        .select(col(xAxisName), col(hiSeriesName), col(lowSeriesName), col(hiSeriesDiff), col(lowSeriesDiff))
        .where(col(hiSeriesDiff) =!= 0 || col(lowSeriesDiff) =!= 0)
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last
          val x0 = element0.getDouble(0)
          val hi = element0.getDouble(1)
          val low = element0.getDouble(2)
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

          (x0, hi, low, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .where($"extrema".isNotNull)
    }

    def firstExtrema(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      dataFrame
        .limit(2)
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last

          val x = element0.getDouble(0)

          val currentExtrema = element0.getString(3)
          val nextExtrema = element1.getString(3)

          val currentHi = element0.getDouble(1)
          val nextHi = element1.getDouble(1)

          val currentLow = element0.getDouble(2)
          val nextLow = element1.getDouble(2)

          def duplicate: Boolean = {
            if (currentExtrema == nextExtrema) true else false
          }

          def nextMaxValueExtrema: String = {
            if (currentLow >= nextLow) currentExtrema else null
          }

          def nextMinValueExtrema: String = {
            if (currentHi <= nextHi) currentExtrema else null
          }

          val extrema = if (duplicate) {
            if (currentExtrema == "maxima") {
              nextMaxValueExtrema
            } else {
              nextMinValueExtrema
            }
          } else {
            currentExtrema
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
    }

    def lastExtrema(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      dataFrame
        .orderBy(col(xAxisName).desc).limit(2)
        .orderBy(xAxisName)
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last

          val x = element1.getDouble(0)
          val previousExtrema = element0.getString(3)
          val currentExtrema = element1.getString(3)
          val previousHi = element0.getDouble(1)
          val currentHi = element1.getDouble(1)
          val previousLow = element0.getDouble(2)
          val currentLow = element1.getDouble(2)

          def duplicate: Boolean = {
            if (currentExtrema == previousExtrema) true else false
          }

          def previousMaxValueExtrema: String = {
            if (previousLow < currentLow) currentExtrema else null
          }

          def previousMinValueExtrema: String = {
            if (previousHi > currentHi) currentExtrema else null
          }

          val extrema = if (duplicate) {
            if (currentExtrema == "maxima") {
              previousMaxValueExtrema
            } else {
              previousMinValueExtrema
            }
          } else {
            currentExtrema
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
    }

    def removeDuplicate(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      // FIXME: cache the dataframe
      val df = dataFrame
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .orderBy(xAxisName)

      val first = df.firstExtrema(xAxisName, hiSeriesName, lowSeriesName)
      val last = df.lastExtrema(xAxisName, hiSeriesName, lowSeriesName)

      df
        .rdd
        .sliding(3)
        .map { array =>
          val element0 = array.head
          val element1 = array(1)
          val element2 = array.last

          val x = element1.getDouble(0)

          val previousExtrema = element0.getString(3)
          val currentExtrema = element1.getString(3)
          val nextExtrema = element2.getString(3)

          val previousHi = element0.getDouble(1)
          val currentHi = element1.getDouble(1)
          val nextHi = element2.getDouble(1)

          val previousLow = element0.getDouble(2)
          val currentLow = element1.getDouble(2)
          val nextLow = element2.getDouble(2)

          def duplicate: Boolean = {
            if (currentExtrema == previousExtrema || currentExtrema == nextExtrema) true else false
          }

          def sameAsPrevious: Boolean = {
            if (currentExtrema == previousExtrema) true else false
          }

          def previousMaxValueExtrema: String = {
            if (previousLow < currentLow) currentExtrema else null
          }

          def nextMaxValueExtrema: String = {
            if (currentLow >= nextLow) currentExtrema else null
          }

          def previousMinValueExtrema: String = {
            if (previousHi > currentHi) currentExtrema else null
          }

          def nextMinValueExtrema: String = {
            if (currentHi <= nextHi) currentExtrema else null
          }

          val extrema = if (duplicate) {
            if (currentExtrema == "maxima") {
              if (sameAsPrevious) {
                previousMaxValueExtrema
              } else {
                nextMaxValueExtrema
              }
            } else {
              if (sameAsPrevious) {
                previousMinValueExtrema
              } else {
                nextMinValueExtrema
              }
            }
          } else {
            currentExtrema
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .union(first)
        .union(last)
        .orderBy(xAxisName)
        .where($"extrema".isNotNull)
    }

    def removeUnusedExtremasPass1(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      // FIXME: cache dataFrame!

      dataFrame
        // FIXME: get rid of unused columns
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .orderBy(xAxisName)
        .rdd
        .sliding(3)
        .map { array =>
          val element0 = array.head
          val element2 = array.last
          val x = element0.getDouble(0)

          val currentHi = element0.getDouble(1)
          val currentLow = element0.getDouble(2)
          val element2Hi = element2.getDouble(1)
          val element2Low = element2.getDouble(2)

          val currentExtrema = element0.getString(3)

          val extrema = if (currentExtrema == "maxima") {
            if (currentLow < element2Low) {
              null
            } else {
              currentExtrema
            }
          } else {
            if (currentHi > element2Hi) {
              null
            } else {
              currentExtrema
            }
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .where($"extrema".isNotNull)
        .orderBy(xAxisName)
    }

    def removeUnusedExtremasPass2(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      // FIXME: cache dataFrame!

      dataFrame
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .orderBy(xAxisName)
        .rdd
        .sliding(3)
        .map { array =>
          val element0 = array.head
          val element2 = array.last
          val x = element2.getDouble(0)

          val element0Hi = element0.getDouble(1)
          val element0Low = element0.getDouble(2)
          val currentHi = element2.getDouble(1)
          val currentLow = element2.getDouble(2)

          val currentExtrema = element2.getString(3)

          val extrema = if (currentExtrema == "maxima") {
            if (element0Low < currentLow) {
              currentExtrema
            } else {
              null
            }
          } else {
            if (element0Hi > currentHi) {
              currentExtrema
            } else {
              null
            }
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .where($"extrema".isNotNull)
        .orderBy(xAxisName)
    }
  }

}
