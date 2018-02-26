package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkSpec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, min, sum, when}
import org.scalatest.FunSpec

class CompleteDatasetTaskSpec extends FunSpec with SparkSpec {
  describe("run") {
    it("should return correct extrema") {
      val completeDataset = new CompleteDatasetTask().run(Map(
        "inputFile" → "src/test/resources/data/hi_low_large.csv.gz",
        "header" → true,
        "xAxisName" → "x",
        "hiSeriesName" → "hi",
        "lowSeriesName" → "low"
      ))
        .get
        .withColumn("increment_partition", when(col("extrema").isNotNull, 1).otherwise(0))
        .withColumn("partition", sum(col("increment_partition")).over(Window.orderBy("x")))
        .withColumn("partition_hi", max("low").over(Window.partitionBy("partition")))
        .withColumn("partition_low", min("hi").over(Window.partitionBy("partition")))

      assert(completeDataset.where(col("extrema") === "maxima" && col("partition_hi") =!= col("low")).count() == 0)
      assert(completeDataset.where(col("extrema") === "minima" && col("partition_low") =!= col("hi")).count() == 0)
    }
  }
}
