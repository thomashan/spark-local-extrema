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
    it("findCandidateExtremas should find all candidate extremas") {
      val input = loadCsv("src/test/resources/data/hi_low_diff.csv")
      val expected = DataFrameUtils.setNullableState(
        DataFrameUtils.setNullableStateForAllColumns(
          loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv"), false
        ), true, "extrema")

      val result = input.findCandidateExtremas("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }

    it("removeDuplicate should remove unused extremas") {
      val input = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")
      val expected = DataFrameUtils.setNullableState(DataFrameUtils.setNullableStateForAllColumns(
        loadCsv("src/test/resources/data/hi_low_candidate_extrema_set_deduplicated.csv"), false
      ), true, "extrema")

      val result = input.removeDuplicate("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }
  }

  private def loadCsvs(csvFiles: String*): DataFrame = {
    csvFiles.map(loadCsv(_))
      .reduce((csvFile1, csvFile2) => csvFile1.join("x", csvFile2))
      .orderBy("x")
  }

  private def loadCsv(csvFile: String): DataFrame = {
    new LoadCsvFileTask().run(Map(
      "inputFile" -> csvFile,
      "header" -> true
    )).get.orderBy("x")
  }
}
