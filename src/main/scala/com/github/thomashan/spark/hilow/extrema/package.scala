package com.github.thomashan.spark.hilow

import com.github.thomashan.spark.hilow.diff.DifferentiateTask
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

package object extrema {

  implicit private[hilow] class Methods(dataFrame: DataFrame) {

    import dataFrame.sqlContext.implicits._

    def join(xAxisName: String, extrema: DataFrame): DataFrame = {
      dataFrame
        .join(extrema, Seq(xAxisName), "left")
        .orderBy(xAxisName)
    }

    def getCandidateExtremaFromDiff(extrema: String, xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val seriesName = if (extrema == "maxima") lowSeriesName else hiSeriesName

      dataFrame
        .orderBy(xAxisName)
        .select(xAxisName, hiSeriesName, lowSeriesName, s"diff_${seriesName}")
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last
          val x0 = element0.getDouble(0)
          val hi = element0.getDouble(1)
          val low = element0.getDouble(2)
          val seriesDiff0 = element0.getDouble(3)
          val seriesDiff1 = element1.getDouble(3)
          val extremaValue = extrema match {
            case "maxima" if seriesDiff0 > 0 && seriesDiff1 < 0 => Some("maxima")
            case "minima" if seriesDiff0 < 0 && seriesDiff1 > 0 => Some("minima")
            case _ => None
          }

          (x0, hi, low, extremaValue)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .where(col("extrema").isNotNull)
    }

    def initialMinimaCandidates(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val hiSeriesDiff = "diff_" + hiSeriesName

      dataFrame
        .where(col(hiSeriesDiff) =!= 0)
        .getCandidateExtremaFromDiff("minima", xAxisName, hiSeriesName, lowSeriesName)
        .cache
    }

    def initialMaximaCandidates(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val lowSeriesDiff = "diff_" + lowSeriesName

      dataFrame
        .where(col(lowSeriesDiff) =!= 0)
        .getCandidateExtremaFromDiff("maxima", xAxisName, hiSeriesName, lowSeriesName)
        .cache
    }

    def initialExtremaCandidates(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val initialMinimaCandidates = dataFrame
        .initialMinimaCandidates(xAxisName, hiSeriesName, lowSeriesName)

      val initialMaximaCandidates = dataFrame
        .initialMaximaCandidates(xAxisName, hiSeriesName, lowSeriesName)

      initialMinimaCandidates
        .union(initialMaximaCandidates)
        .orderBy(xAxisName, "extrema")
        .cache
    }

    def extremaCandidatesBasedOn2ndDiff(initialMinimaCandidates: DataFrame, initialMaximaCandidates: DataFrame, xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val hiSeriesDiff = "diff_" + hiSeriesName
      val lowSeriesDiff = "diff_" + lowSeriesName

      implicit val spark = dataFrame.sparkSession

      val minimaCandidate: DataFrame = new DifferentiateTask().run(Map(
        "input" -> initialMinimaCandidates,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      )).getOrElse(throw new RuntimeException)
        .where(col(hiSeriesDiff) =!= 0)
        .getCandidateExtremaFromDiff("minima", xAxisName, hiSeriesName, lowSeriesName)

      val maximaCandidate: DataFrame = new DifferentiateTask().run(Map(
        "input" -> initialMaximaCandidates,
        "xAxisName" -> xAxisName,
        "hiSeriesName" -> hiSeriesName,
        "lowSeriesName" -> lowSeriesName
      )).getOrElse(throw new RuntimeException)
        .where(col(lowSeriesDiff) =!= 0)
        .getCandidateExtremaFromDiff("maxima", xAxisName, hiSeriesName, lowSeriesName)

      minimaCandidate.union(maximaCandidate)
    }

    def extremaCandidatesBasedOnRollingWindow(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val extremaCandidates1 = dataFrame
        .groupBy(xAxisName, hiSeriesName, lowSeriesName)
        .agg(first("extrema").as("extrema"))
        .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)
        .removeUnusedExtrema(xAxisName, hiSeriesName, lowSeriesName, 0)

      val extremaCandidates2 = dataFrame
        .groupBy(xAxisName, hiSeriesName, lowSeriesName)
        .agg(last("extrema").as("extrema"))
        .removeDuplicate(xAxisName, hiSeriesName, lowSeriesName)
        .removeUnusedExtrema(xAxisName, hiSeriesName, lowSeriesName, 0)

      extremaCandidates1
        .union(extremaCandidates2)
        .orderBy(xAxisName)
        .distinct
    }

    def findCandidateExtrema(xAxisName: String, hiSeriesName: String, lowSeriesName: String): DataFrame = {
      val hiSeriesDiff = "diff_" + hiSeriesName
      val lowSeriesDiff = "diff_" + lowSeriesName

      val diff = dataFrame
        .select(xAxisName, hiSeriesName, lowSeriesName, hiSeriesDiff, lowSeriesDiff)
        .cache

      val initialMinimaCandidates = diff
        .initialMinimaCandidates(xAxisName, hiSeriesName, lowSeriesName)
      val initialMaximaCandidates = diff
        .initialMaximaCandidates(xAxisName, hiSeriesName, lowSeriesName)
      val initialExtremaCandidates1 = initialMinimaCandidates
        .union(initialMaximaCandidates)
        .orderBy(xAxisName, "extrema")
        .cache

      val extremaCandidates1 = diff
        .extremaCandidatesBasedOn2ndDiff(initialMinimaCandidates, initialMaximaCandidates, xAxisName, hiSeriesName, lowSeriesName)
      val extremaCandidates2 = initialExtremaCandidates1
        .extremaCandidatesBasedOnRollingWindow(xAxisName, hiSeriesName, lowSeriesName)

      val result = extremaCandidates1
        .union(extremaCandidates2)
        .orderBy(xAxisName)
        .distinct

      result
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

          def duplicate: Boolean = currentExtrema == nextExtrema

          def nextMaxValueExtrema: Option[String] = {
            if (currentLow >= nextLow) Some(currentExtrema) else None
          }

          def nextMinValueExtrema: Option[String] = {
            if (currentHi <= nextHi) Some(currentExtrema) else None
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

          def previousMaxValueExtrema: Option[String] = {
            if (previousLow < currentLow) Some(currentExtrema) else None
          }

          def previousMinValueExtrema: Option[String] = {
            if (previousHi > currentHi) Some(currentExtrema) else None
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
      dataFrame
        .withColumn("extrema_value", when($"extrema" === "maxima", col(lowSeriesName)).otherwise(col(hiSeriesName)))
        .withColumn("previous_extrema", lag("extrema", 1).over(Window.orderBy(xAxisName)))
        .withColumn("increment", when($"extrema" =!= $"previous_extrema", 1).otherwise(0))
        .withColumn("partition", sum("increment").over(Window.orderBy(xAxisName)))
        .withColumn("partition_extrema_min_value", min("extrema_value").over(Window.partitionBy("partition")))
        .withColumn("partition_extrema_max_value", max("extrema_value").over(Window.partitionBy("partition")))
        .withColumn("partition_extrema_value", when($"extrema" === "maxima", $"partition_extrema_max_value").otherwise($"partition_extrema_min_value"))
        .where($"partition_extrema_value" === $"extrema_value")
        .withColumn("row_in_partition", row_number().over(Window.orderBy(xAxisName).partitionBy("partition")))
        .where($"row_in_partition" === 1)
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")

    }

    def removeUnusedExtrema(xAxisName: String, hiSeriesName: String, lowSeriesName: String, minimumDistance: Double): DataFrame = {
      val pass1 = dataFrame.removeUnusedExtremaPass1(xAxisName, hiSeriesName, lowSeriesName, minimumDistance)
      val pass2 = dataFrame.removeUnusedExtremaPass2(xAxisName, hiSeriesName, lowSeriesName, minimumDistance)

      dataFrame
        .join(pass1, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")
        .join(pass2, Seq(xAxisName, hiSeriesName, lowSeriesName), "left")
        .withColumn("extrema", when($"extrema_pass1".isNotNull && $"extrema_pass2".isNotNull, $"extrema_pass1"))
        .select(xAxisName, hiSeriesName, lowSeriesName, "extrema")
        .union(pass1.orderBy(xAxisName).limit(2).select(col(xAxisName), col(hiSeriesName), col(lowSeriesName), $"extrema_pass1").as("extrema"))
        .union(pass2.orderBy(col(xAxisName).desc).limit(2).select(col(xAxisName), col(hiSeriesName), col(lowSeriesName), $"extrema_pass2").as("extrema"))
        .where($"extrema".isNotNull)
        .orderBy(xAxisName)
    }

    def removeUnusedExtremaPass1(xAxisName: String, hiSeriesName: String, lowSeriesName: String, minimumDistance: Double): DataFrame = {
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
              case currentLow if currentLow > (element1Hi + minimumDistance) && element1Hi < element2Low => Some(currentExtrema)
              case currentLow if currentLow < element2Low => None
              case _ => Some(currentExtrema)
            }
            case "minima" => currentHi match {
              case currentHi if currentHi < (element1Low - minimumDistance) && element1Low > element2Hi => Some(currentExtrema)
              case currentHi if currentHi > element2Hi => None
              case _ => Some(currentExtrema)
            }
            case _ => None
          }

          (x, currentHi, currentLow, extrema)
        }
        .toDF(xAxisName, hiSeriesName, lowSeriesName, "extrema_pass1")
        .where($"extrema_pass1".isNotNull)
        .orderBy(xAxisName)
    }

    def removeUnusedExtremaPass2(xAxisName: String, hiSeriesName: String, lowSeriesName: String, minimumDistance: Double): DataFrame = {
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
              case element0Low if element0Low > element1Hi && (element1Hi + minimumDistance) < currentLow => Some(currentExtrema)
              case element0Low if element0Low < currentLow => Some(currentExtrema)
              case _ => None
            }
            case "minima" => element0Hi match {
              case element0Hi if element0Hi < element1Low && (element1Low - minimumDistance) > currentHi => Some(currentExtrema)
              case element0Hi if element0Hi > currentHi => Some(currentExtrema)
              case _ => None
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
