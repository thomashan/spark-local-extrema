package com.github.thomashan.spark.hilow

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

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

          val extrema = duplicate match {
            case true => currentExtrema match {
              case "maxima" => nextMaxValueExtrema
              case "minima" => nextMinValueExtrema
            }
            case false => currentExtrema
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

          val extrema = duplicate match {
            case true => currentExtrema match {
              case "maxima" => previousMaxValueExtrema
              case "minima" => previousMinValueExtrema
            }
            case false => currentExtrema
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

          def minValueExtrema: String = {
            if (previousHi > currentHi && currentHi <= nextHi) currentExtrema else null
          }

          def maxValueExtrema: String = {
            if (previousLow < currentLow && currentLow >= nextLow) currentExtrema else null
          }

          val extrema = duplicate match {
            case true => currentExtrema match {
              case "maxima" => currentExtrema match {
                case currentExtrema if currentExtrema == previousExtrema && currentExtrema == nextExtrema => maxValueExtrema
                case currentExtrema if currentExtrema == previousExtrema => previousMaxValueExtrema
                case _ => nextMaxValueExtrema
              }
              case "minima" => currentExtrema match {
                case currentExtrema if currentExtrema == previousExtrema && currentExtrema == nextExtrema => minValueExtrema
                case currentExtrema if currentExtrema == previousExtrema => previousMinValueExtrema
                case _ => nextMinValueExtrema
              }
            }
            case false => currentExtrema
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .union(first)
        .union(last)
        .orderBy(xAxisName)
        .where($"extrema".isNotNull)
    }

    def removeUnusedExtremas(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val pass1 = dataFrame.removeUnusedExtremasPass1(xAxisName, hiSeriesName, lowSeriesName)
      val pass2 = dataFrame.removeUnusedExtremasPass2(xAxisName, hiSeriesName, lowSeriesName)

      dataFrame
        .join(pass1, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")
        .join(pass2, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")
        .withColumn("extrema", when($"extrema_pass1".isNotNull && $"extrema_pass2".isNotNull, $"extrema_pass1"))
        .where($"extrema".isNotNull)
        .orderBy()
    }

    def removeUnusedExtremasPass1(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      // FIXME: cache dataFrame!

      dataFrame
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .orderBy(xAxisName)
        .rdd
        .sliding(3)
        .map { array =>
          val element0 = array.head
          val element1 = array(1)
          val element2 = array.last
          val x = element0.getDouble(0)

          val currentHi = element0.getDouble(1)
          val currentLow = element0.getDouble(2)
          val element1Hi = element1.getDouble(1)
          val element1Low = element1.getDouble(2)
          val element2Hi = element2.getDouble(1)
          val element2Low = element2.getDouble(2)

          val currentExtrema = element0.getString(3)

          val extrema = currentExtrema match {
            case "maxima" => currentLow match {
              case currentLow if currentLow > element1Hi && element1Hi < element1Low => currentExtrema
              case currentLow if currentLow < element2Low => null
              case _ => currentExtrema
            }
            case "minima" => currentHi match {
              case currentHi if currentHi < element1Low && element1Low > element2Hi => currentExtrema
              case currentHi if currentHi > element2Hi => null
              case _ => currentExtrema
            }
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema_pass1")
        .where($"extrema_pass1".isNotNull)
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
          val element1 = array(1)
          val element2 = array.last
          val x = element2.getDouble(0)

          val element0Hi = element0.getDouble(1)
          val element0Low = element0.getDouble(2)
          val element1Hi = element1.getDouble(1)
          val element1Low = element1.getDouble(2)
          val currentHi = element2.getDouble(1)
          val currentLow = element2.getDouble(2)

          val currentExtrema = element2.getString(3)

          val extrema = currentExtrema match {
            case "maxima" => element0Low match {
              case element0Low if element0Low > element1Hi && element1Hi < currentLow => currentExtrema
              case element0Low if element0Low < currentLow => currentExtrema
              case _ => null
            }
            case "minima" => element0Hi match {
              case element0Hi if element0Hi < element1Low && element1Low > currentHi => currentExtrema
              case element0Hi if element0Hi > currentHi => currentExtrema
              case _ => null
            }
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema_pass2")
        .where($"extrema_pass2".isNotNull)
        .orderBy(xAxisName)
    }
  }

}
