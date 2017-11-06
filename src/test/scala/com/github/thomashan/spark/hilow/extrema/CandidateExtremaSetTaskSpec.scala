package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.SparkSpec
import com.github.thomashan.spark.common.LoadCsvFileTask
import org.apache.spark.sql.DataFrame


class CandidateExtremaSetTaskSpec extends SparkSpec {
  var reducedExtremaSetTask: CandidateExtremaSetTask = _

  before {
    reducedExtremaSetTask = new CandidateExtremaSetTask()
  }

  describe("implementation details") {
    it("findCandidateExtremas should find all candidate extremas") {
      val input = loadCsv("src/test/resources/data/hi_low_diff.csv")
      val expected = loadCsv("src/test/resources/data/hi_low_candidate_extrema_set.csv")

      val result = input.findCandidateExtremas("x", "hi", "low")

      assertDataFrameEquals(expected, result)
    }

    it("removeDuplicate should remove unused extremas") {
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
