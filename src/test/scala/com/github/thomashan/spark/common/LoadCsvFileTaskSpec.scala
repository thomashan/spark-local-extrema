package com.github.thomashan.spark.common

import com.github.thomashan.spark.SparkSpec
import org.scalatest.{Outcome, fixture}

class LoadCsvFileTaskSpec extends fixture.FunSpec with SparkSpec {
  type FixtureParam = LoadCsvFileTask

  override def withFixture(test: OneArgTest): Outcome = {
    val loadCsvFileTask = new LoadCsvFileTask()
    test(loadCsvFileTask)
  }

  describe("run") {
    it("should be able to read test file") { loadCsvFileTask =>
      val dataframeOption = loadCsvFileTask.run(
        Map(
          "inputFile" -> "src/test/resources/data/cartesian_points.csv",
          "header" -> true
        )
      )

      assert(dataframeOption.isDefined)
      assert(dataframeOption.get.collect().length == 13)
    }
  }
}
