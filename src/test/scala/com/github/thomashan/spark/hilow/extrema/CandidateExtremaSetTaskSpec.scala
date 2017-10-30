package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.common.LoadCsvFileTask
import com.github.thomashan.spark.{DataFrameUtils, SparkSpec}
import org.apache.spark.sql.DataFrame


class CandidateExtremaSetTaskSpec extends SparkSpec {
  var reducedExtremaSetTask: CandidateExtremaSetTask = _

  before {
    reducedExtremaSetTask = new CandidateExtremaSetTask()
  }

  describe("implementation details") {
    it("findCrossovers should calculate cross overs") {
      val input = loadCsv("src/test/resources/data/hi_low_diff.csv")
      val expected = DataFrameUtils.setNullableState(
        DataFrameUtils.setNullableStateForAllColumns(
          loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv"), false
        ), true, "extrema")

      val result = input.findCrossovers("x", "hi", "low")

      expected.printSchema
      result.printSchema


      assertDataFrameEquals(expected, result)
    }

    it("removeDuplicateExtremas should remove duplicate extremas") {
      val input = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
      val expected = DataFrameUtils.setNullableStateForAllColumns(
        loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_duplicated_removed.csv"), true
      )

      val result = input.removeDuplicateExtremas("x", "hi", "low")

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
