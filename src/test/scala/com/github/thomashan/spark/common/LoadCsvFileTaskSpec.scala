package com.github.thomashan.spark.common

import com.github.thomashan.spark.SparkSpec

class LoadCsvFileTaskSpec extends SparkSpec {
  var loadCsvFileTask: LoadCsvFileTask = _

  before {
    loadCsvFileTask = new LoadCsvFileTask()
  }

  describe("run") {
    it("should be able to read test file") {
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
