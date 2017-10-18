package com.github.thomashan.spark.diff

import com.github.thomashan.spark.{DataFrameUtils, SparkSpec}
import org.apache.spark.sql.DataFrame

class DifferentiateSpec extends SparkSpec {

  var differentiateTask: DifferentiateTask = _

  before {
    differentiateTask = new DifferentiateTask()
  }

  describe("perform differentiation") {
    it("run should produce correct diff values") {
      val input = loadCsvFile("src/test/resources/data/cartesian_points.csv")
      val expected = DataFrameUtils.setNullableStateForAllColumns(loadCsvFile("src/test/resources/data/cartesian_points_diff.csv"), false)

      val differentiated = differentiateTask.run(
        Map(
          "input" -> input,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get

      assert(differentiated.collect.length == 8)
      assertDataFrameEquals(expected, differentiated)
    }
  }

  private def loadCsvFile(path: String): DataFrame = {
    spark.read.option("header", true).option("inferSchema", true).csv(path)
  }
}
