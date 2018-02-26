package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.{File, SparkSpec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}
import org.scalatest.FunSpec

class TrueExtremaInPartitionTaskSpec extends FunSpec with SparkSpec {
  describe("run") {
    it("should return same number of partitions") {
      val input = File.loadCsv("src/test/resources/data/hi_low.csv")
      val extremaSet = new RemoveUnusedExtremaTask(0).run(Map(
        "extrema_deduped" -> File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_deduplicated.csv"),
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      )).get

      val trueExtremaSet = new TrueExtremaInPartitionTask(input, extremaSet).run(Map(
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      )).get

      assert(extremaSet.count == trueExtremaSet.count)
    }

    it("should return extrema for all partitions") {
      val input = File.loadCsv("src/test/resources/data/hi_low.csv")
      val extremaSet = new RemoveUnusedExtremaTask(0).run(Map(
        "extrema_deduped" -> File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_deduplicated.csv"),
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      )).get
        .withColumn("increment_partition", lit(1))
        .withColumn("partition", sum(col("increment_partition")).over(Window.orderBy("x")))

      val trueExtremaSet = new TrueExtremaInPartitionTask(input, extremaSet).run(Map(
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      )).get
        .withColumn("increment_partition", lit(1))
        .withColumn("partition", sum(col("increment_partition")).over(Window.orderBy("x")))

      assert(extremaSet.join(trueExtremaSet, Seq("partition", "extrema"), "left").count == extremaSet.count)
    }
  }
}
