package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkJob
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class TempJob extends SparkJob {
  override val applicationName: String = getClass.getName

  import spark.implicits._

  override protected def run(args: Array[String]): Unit = {
    val hiLow = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/data/hi_low_big.csv.gz")

    val diffs = hiLow
      .select("x", "hi", "low")
      .orderBy("x")
      .rdd
      .sliding(2)
      .map { array =>
        val element0 = array.head
        val element1 = array.last
        val x0 = element0.getDouble(0)
        val x1 = element1.getDouble(0)
        val hiSeries0 = element0.getDouble(1)
        val hiSeries1 = element1.getDouble(1)
        val lowSeries0 = element0.getDouble(2)
        val lowSeries1 = element1.getDouble(2)

        val hiSeriesDiff = (hiSeries1 - hiSeries0) / (x1 - x0)
        val lowSeriesDiff = (lowSeries1 - lowSeries0) / (x1 - x0)

        (x1, hiSeries1, lowSeries1, hiSeriesDiff, lowSeriesDiff)
      }
      .toDF("x", "hi", "low", "diff_hi", "diff_low")

    val hiLowDiff = hiLow
      .join(diffs, Seq("x", "hi", "low"), "left")
      .orderBy("x")

    val candidateExtremas = hiLowDiff
      .select("x", "hi", "low", "diff_hi", "diff_low")
      .where($"diff_hi" =!= 0 || $"diff_low" =!= 0)
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
      .toDF("x", "hi", "low", "extrema")
      .where($"extrema".isNotNull)
      .orderBy("x")

    val extremasDuplicateRemoved = candidateExtremas
      .withColumn("extrema_value", when($"extrema" === "maxima", $"low").otherwise($"hi"))
      .withColumn("previous_extrema", lag($"extrema", 1).over(Window.orderBy($"x")))
      .withColumn("increment", when($"extrema" =!= $"previous_extrema", 1).otherwise(0))
      .withColumn("partition", sum($"increment").over(Window.orderBy("x")))
      .withColumn("partition_extrema_min_value", min("extrema_value").over(Window.partitionBy("partition")))
      .withColumn("partition_extrema_max_value", max("extrema_value").over(Window.partitionBy("partition")))
      .withColumn("partition_extrema_value", when($"extrema" === "maxima", $"partition_extrema_max_value").otherwise($"partition_extrema_min_value"))
      .where($"partition_extrema_value" === $"extrema_value")
      .withColumn("row_in_partition", row_number().over(Window.orderBy("x").partitionBy("partition")))
      .select("x", "hi", "low", "extrema")
      .where($"row_in_partition" === 1)

    extremasDuplicateRemoved.count
  }
}

object TempJob extends App {
  new TempJob().run(args)
}
