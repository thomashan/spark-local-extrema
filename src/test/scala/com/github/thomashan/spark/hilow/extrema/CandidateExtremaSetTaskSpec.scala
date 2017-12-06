package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.common.LoadCsvFileTask
import com.github.thomashan.spark.{SparkSpec, _}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec

class CandidateExtremaSetTaskSpec extends FunSpec with SparkSpec {
  describe("implementation details") {
    it("findCandidateExtrema should find all candidate extrema") {
      val input = loadCsv("src/test/resources/data/hi_low_diff.csv")
      val expected = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
        .setNullableForAllColumns(false)
        .setNullable(true, "extrema")

      val result = input.findCandidateExtrema("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }

    it("removeDuplicate should remove unused extrema") {
      val input = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
      val expected = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_deduplicated.csv")

      val result = input.removeDuplicate("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }
  }

  private def loadCsv(csvFile: String): DataFrame = {
    new LoadCsvFileTask().run(Map(
      "inputFile" -> csvFile,
      "header" -> true
    )).get.orderBy("x")
  }
}
