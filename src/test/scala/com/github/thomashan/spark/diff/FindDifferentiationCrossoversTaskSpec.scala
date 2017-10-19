package com.github.thomashan.spark.diff

import com.github.thomashan.spark.{DataFrameUtils, SparkSpec}
import org.apache.spark.sql.functions._

class FindDifferentiationCrossoversTaskSpec extends SparkSpec {
  var findDifferentiationCrossoversTask: FindDifferentiationCrossoversTask = _

  import spark.implicits._

  before {
    findDifferentiationCrossoversTask = new FindDifferentiationCrossoversTask()
  }

  describe("implementation details") {
    it("allExtremas should return all extrema points") {
      val input = Seq(
        (0.5, 0.5, 1d, null, null),
        (1d, 1d, 1d, "maxima", java.lang.Long.valueOf(0)),
        (1.5, 1d, 0d, null, null),
        (2d, 0.5, -1d, null, null),
        (2.5, 0d, -1d, "minima", java.lang.Long.valueOf(1)),
        (3d, 0d, 0d, null, null),
        (3.5, 0d, 0d, null, null),
        (4d, 0.5, 1d, "maxima", java.lang.Long.valueOf(2)),
        (5d, -0.5, -1d, null, null),
        (5.1, -0.5, 0d, null, null),
        (5.2, -0.5, 0d, null, null),
        (5.5, -1d, -1.6666666666666676, null, null)
      ).toDF("x", "y", "diff", "extrema", "extrema_index")
      val expected = Seq(
        (1d, 1d, 1d, "maxima", java.lang.Long.valueOf(0)),
        (1.5, 1d, 0d, "maxima", java.lang.Long.valueOf(0)),
        (2.5, 0d, -1d, "minima", java.lang.Long.valueOf(1)),
        (3d, 0d, 0d, "minima", java.lang.Long.valueOf(1)),
        (3.5, 0d, 0d, "minima", java.lang.Long.valueOf(1)),
        (4d, 0.5, 1d, "maxima", java.lang.Long.valueOf(2))
      ).toDF("x", "y", "diff", "extrema", "extrema_index")

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("findCrossovers should return all crossovers") {
      val input = Seq(
        (0.5, 0.5, 1d),
        (1d, 1d, 1d),
        (2d, 0.5, -1d),
        (2.5, 0d, -1d),
        (4d, 0.5, 1d),
        (5d, -0.5, -1d),
        (5.5, -1d, -1.6666666666666676)
      ).toDF("x", "y", "diff")
      val expected = Seq(
        (1d, 1d, 1d, "maxima"),
        (2.5, 0d, -1d, "minima"),
        (4d, 0.5, 1d, "maxima")
      ).toDF("x", "y", "diff", "extrema")

      val result = input.findCrossovers("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("crossoverIndex should return crossover index") {
      val input = Seq(
        (1d, 1d, 1d, "maxima"),
        (2.5, 0d, -1d, "minima"),
        (4d, 0.5, 1d, "maxima")
      ).toDF("x", "y", "diff", "extrema")
      val expected = Seq(
        (1d, 1d, 1d, "maxima", 0l),
        (2.5, 0d, -1d, "minima", 1l),
        (4d, 0.5, 1d, "maxima", 2l)
      ).toDF("x", "y", "diff", "extrema", "extrema_index")

      val result = input.crossoverIndex("x", "y")

      assertDataFrameEquals(expected, result)
    }
  }

  describe("find differentiation crossover") {
    it("run should produce correct extrema types") {
      val input = loadCsvFile("src/test/resources/data/cartesian_points.csv")

      val u = udf((i: Int) => i.toLong)
      val expected = DataFrameUtils.setNullableStateForAllColumns(loadCsvFile("src/test/resources/data/cartesian_points_crossover_points.csv")
        .withColumn("temp_extrema_index", u($"extrema_index"))
        .select($"x", $"y", $"extrema", $"temp_extrema_index".as("extrema_index")), true)

      val crossovers = findDifferentiationCrossoversTask.run(
        Map(
          "input" -> input,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get


      assertDataFrameEquals(expected, crossovers)
    }
  }
}
