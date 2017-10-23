package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.{DataFrameUtils, SparkSpec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class CompleteExtremaSetTaskSpec extends SparkSpec {
  var extremaSetTask: CompleteExtremaSetTask = _

  import spark.implicits._

  before {
    extremaSetTask = new CompleteExtremaSetTask()
  }

  describe("implementation details") {
    it("allExtremas should return all extrema points simple scenario 1") {
      val input = prepareCompleteDataset(
        (0, 0, null, null, null),
        (1, 1, 1.0, "maxima", 0l),
        (2, -1, -2.0, "minima", 1l),
        (3, 0, 1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (1, 1, 1.0, "maxima", 0l),
        (2, -1, -2.0, "minima", 1l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("allExtremas should return all extrema points simple scenario 2") {
      val input = prepareCompleteDataset(
        (0, 0, null, null, null),
        (1, -1, -1.0, "minima", 0l),
        (2, 1, 2.0, "maxima", 1l),
        (3, 0, -1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (1, -1, -1.0, "minima", 0l),
        (2, 1, 2.0, "maxima", 1l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("allExtremas should return all extrema points complex scenario 1") {
      val input = prepareCompleteDataset(
        (1, 0, null, null, null),
        (2, 1, 1.0, "maxima", 0l),
        (3, 1, 0.0, "maxima", 0l),
        (4, 1, 0.0, "maxima", 0l),
        (5, 1, 0.0, "maxima", 0l),
        (6, 0, -1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (2, 1, 1.0, "maxima", 0l),
        (3, 1, 0.0, "maxima", 0l),
        (4, 1, 0.0, "maxima", 0l),
        (5, 1, 0.0, "maxima", 0l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("allExtremas should return all extrema points complex scenario 2") {
      val input = prepareCompleteDataset(
        (1, 0, null, null, null),
        (2, 1, -1.0, "minima", 0l),
        (3, 1, 0.0, "minima", 0l),
        (4, 1, 0.0, "minima", 0l),
        (5, 1, 0.0, "minima", 0l),
        (6, 0, 1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (2, 1, -1.0, "minima", 0l),
        (3, 1, 0.0, "minima", 0l),
        (4, 1, 0.0, "minima", 0l),
        (5, 1, 0.0, "minima", 0l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("allExtremas should return all extrema points complex scenario 3") {
      val input = prepareCompleteDataset(
        (0, 0, null, null, null),
        (1, 1, 1.0, "maxima", 0l),
        (2, 0, -1.0, "minima", 1l),
        (3, 0.5, 0.5, null, null),
        (4, 0.5, 0.0, null, null),
        (5, 0.5, 0.0, null, null),
        (6, 1, 0.5, "maxima", 2l),
        (7, 0, -1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (1, 1, 1.0, "maxima", 0l),
        (2, 0, -1.0, "minima", 1l),
        (6, 1, 0.5, "maxima", 2l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

    it("allExtremas should return all extrema points complex scenario 4") {
      val input = prepareCompleteDataset(
        (0, 0, null, null, null),
        (1, -1, -1.0, "maxima", 0l),
        (2, 0, 1.0, "minima", 1l),
        (3, -0.5, -0.5, null, null),
        (4, -0.5, 0.0, null, null),
        (5, -0.5, 0.0, null, null),
        (6, -1, -0.5, "maxima", 2l),
        (7, 0, 1.0, null, null)
      )
      val expected = prepareCompleteDataset(
        (1, -1, -1.0, "maxima", 0l),
        (2, 0, 1.0, "minima", 1l),
        (6, -1, -0.5, "maxima", 2l)
      )

      val result = input.allExtremas("x", "y")

      assertDataFrameEquals(expected, result)
    }

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
      val u = udf((i: Int) => i.toLong)
      val diff = loadCsvFile("src/test/resources/data/cartesian_points_diff.csv")
      val reducedExtremaSet = loadCsvFile("src/test/resources/data/cartesian_points_reduced_extrema_set.csv")
        .withColumn("extrema_index", u($"extrema_index"))

      val expected = DataFrameUtils.setNullableStateForAllColumns(loadCsvFile("src/test/resources/data/cartesian_points_extrema_set.csv")
        .withColumn("extrema_index", u($"extrema_index")), true)

      val crossovers = extremaSetTask.run(
        Map(
          "diff" -> diff,
          "reducedExtremaSet" -> reducedExtremaSet,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get


      assertDataFrameEquals(expected, crossovers)
    }
  }

  private def prepareCompleteDataset(rows: (Double, Double, java.lang.Double, String, java.lang.Long)*): DataFrame = {
    rows.toDF("x", "y", "diff", "extrema", "extrema_index")
  }
}
