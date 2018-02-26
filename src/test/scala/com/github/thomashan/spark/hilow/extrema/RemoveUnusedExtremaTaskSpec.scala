package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.{File, SparkSpec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.scalatest.FunSpec

class RemoveUnusedExtremaTaskSpec extends FunSpec with SparkSpec {
  describe("run") {
    it("should return correct extrema") {
      val input = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
        .removeDuplicate("x", "hi", "low")

      val result = new RemoveUnusedExtremaTask(0).run(Map(
        "extrema_deduped" → input,
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      )).get
        .withColumn("previous_extrema", lag("extrema", 1).over(Window.orderBy("x")))
        .withColumn("previous_hi", lag("hi", 1).over(Window.orderBy("x")))
        .withColumn("previous_low", lag("low", 1).over(Window.orderBy("x")))

      assert(result.where(col("extrema") === "maxima" && col("previous_hi") > col("low")).count == 0)
      assert(result.where(col("extrema") === "minima" && col("previous_low") < col("hi")).count == 0)
    }
  }
}
