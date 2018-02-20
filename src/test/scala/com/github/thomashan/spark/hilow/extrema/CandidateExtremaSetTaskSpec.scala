package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.{SparkSpec, _}
import org.scalatest.FunSpec

class CandidateExtremaSetTaskSpec extends FunSpec with SparkSpec {
  describe("implementation details") {
    it("findCandidateExtrema should find all candidate extrema") {
      val input = File.loadCsv("src/test/resources/data/hi_low_diff.csv")
      val expected = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
        .setNullableForAllColumns(false)
        .setNullable(true, "extrema")

      val result = input.findCandidateExtrema("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }

    it("removeDuplicate should remove unused extrema") {
      val input = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
      val expected = File.loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_deduplicated.csv")

      val result = input.removeDuplicate("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }
  }
}
