package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.{File, SparkSpec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.scalatest.FunSpec

class PackageSpec extends FunSpec with SparkSpec {
  describe("implementation details") {
    describe("removeDuplicate") {
      it("should remove duplicates extrema") {
        val input = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
        assert(input.withColumn("previous_extrema", lag("extrema", 1).over(Window.orderBy("x"))).count != 0)

        val result = input
          .removeDuplicate("x", "hi", "low")
          .withColumn("previous_extrema", lag("extrema", 1).over(Window.orderBy("x")))

        assert(result.where(col("extrema") === col("previous_extrema")).count == 0)
      }
    }

    describe("removeUnusedExtrema") {
      it("should remove unused extrema") {
        val input = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
          .removeDuplicate("x", "hi", "low")

        val result = input
          .removeUnusedExtrema("x", "hi", "low", 0)

        assert(input.count() > result.count())
      }
    }
  }
}
