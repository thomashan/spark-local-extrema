package com.github.thomashan.spark.diff

import com.github.thomashan.spark.{DataFrameUtils, SparkSpec}

class FindDifferentiationCrossoversTaskSpec extends SparkSpec {
  var findDifferentiationCrossoversTask: FindDifferentiationCrossoversTask = _

  import spark.implicits._

  before {
    findDifferentiationCrossoversTask = new FindDifferentiationCrossoversTask()
  }

  describe("find differentiation crossover") {
    it("run should produce correct extrema types") {
      val input = loadCsvFile("src/test/resources/data/cartesian_points_diff.csv")
      val expected = DataFrameUtils.setNullableState(loadCsvFile("src/test/resources/data/cartesian_points_crossover_points.csv"), false, "x", "y", "diff")

      val crossovers = findDifferentiationCrossoversTask.run(
        Map(
          "input" -> input,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get

      assert(crossovers.collect.length == 11)
      assert(crossovers.where($"extrema".isNotNull).collect.length == 6)
      assertDataFrameEquals(expected, crossovers)
    }
  }
}
