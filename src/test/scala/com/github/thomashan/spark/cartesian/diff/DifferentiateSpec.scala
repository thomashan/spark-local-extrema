package com.github.thomashan.spark.cartesian.diff

import com.github.thomashan.spark.{SparkSpec, _}
import org.scalatest.{Outcome, fixture}

class DifferentiateSpec extends fixture.FunSpec with SparkSpec {
  type FixtureParam = DifferentiateTask

  override def withFixture(test: OneArgTest): Outcome = {
    val differentiateTask = new DifferentiateTask()
    test(differentiateTask)
  }

  describe("perform differentiation") {
    it("run should produce correct diff values") { differentiateTask =>
      val input = File.loadCsv("src/test/resources/data/cartesian_points.csv")
      val expected = File.loadCsv("src/test/resources/data/cartesian_points_diff.csv")
        .setNullableForAllColumns(false)
        .setNullable(true, "extrema")

      val differentiated = differentiateTask.run(
        Map(
          "input" -> input,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get

      assert(differentiated.collect.length == 12)
      assertDataFrameEquals(expected, differentiated)
    }
  }
}
