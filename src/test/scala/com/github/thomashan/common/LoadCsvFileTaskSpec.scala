package com.github.thomashan.common

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.scalatest.{BeforeAndAfter, FunSpec}

class LoadCsvFileTaskSpec extends FunSpec with DatasetSuiteBase with BeforeAndAfter {
  var loadCsvFileTask: LoadCsvFileTask = _
  override lazy implicit val spark = SparkSessionProvider._sparkSession

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
      assert(dataframeOption.get.collect().length == 9)
    }
  }
}
