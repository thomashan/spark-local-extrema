package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.{SparkSpec, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{currentRow, unboundedPreceding}
import org.apache.spark.sql.functions._
import org.scalatest.{Outcome, fixture}

class CompleteExtremaSetTaskSpec extends fixture.FunSpec with SparkSpec {

  import spark.implicits._

  type FixtureParam = CompleteExtremaSetTask


  override def withFixture(test: OneArgTest): Outcome = {
    val completeExtremaSetTask = new CompleteExtremaSetTask()
    test(completeExtremaSetTask)
  }

  describe("implementation details") {
    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points simple scenario 1") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(0), Some(0), None, None, None),
        (Some(1), Some(1), Some(1.0), Some("maxima"), Some(0l)),
        (Some(2), Some(-1), Some(-2.0), Some("minima"), Some(1l)),
        (Some(3), Some(0), Some(1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(1), Some(1), Some("maxima"), Some(0l)),
        (Some(2), Some(-1), Some("minima"), Some(1l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points simple scenario 2") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(0), Some(0), None, None, None),
        (Some(1), Some(-1), Some(-1.0), Some("minima"), Some(0l)),
        (Some(2), Some(1), Some(2.0), Some("maxima"), Some(1l)),
        (Some(3), Some(0), Some(-1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(1), Some(-1), Some("minima"), Some(0l)),
        (Some(2), Some(1), Some("maxima"), Some(1l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points complex scenario 1") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(1), Some(0), None, None, None),
        (Some(2), Some(1), Some(1.0), Some("maxima"), Some(0l)),
        (Some(3), Some(1), Some(0.0), Some("maxima"), Some(0l)),
        (Some(4), Some(1), Some(0.0), Some("maxima"), Some(0l)),
        (Some(5), Some(1), Some(0.0), Some("maxima"), Some(0l)),
        (Some(6), Some(0), Some(-1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(2), Some(1), Some("maxima"), Some(0l)),
        (Some(3), Some(1), Some("maxima"), Some(0l)),
        (Some(4), Some(1), Some("maxima"), Some(0l)),
        (Some(5), Some(1), Some("maxima"), Some(0l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points complex scenario 2") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(1), Some(0), None, None, None),
        (Some(2), Some(1), Some(-1.0), Some("minima"), Some(0l)),
        (Some(3), Some(1), Some(0.0), Some("minima"), Some(0l)),
        (Some(4), Some(1), Some(0.0), Some("minima"), Some(0l)),
        (Some(5), Some(1), Some(0.0), Some("minima"), Some(0l)),
        (Some(6), Some(0), Some(1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(2), Some(1), Some("minima"), Some(0l)),
        (Some(3), Some(1), Some("minima"), Some(0l)),
        (Some(4), Some(1), Some("minima"), Some(0l)),
        (Some(5), Some(1), Some("minima"), Some(0l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points complex scenario 3") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(0), Some(0), None, None, None),
        (Some(1), Some(1), Some(1.0), Some("maxima"), Some(0l)),
        (Some(2), Some(0), Some(-1.0), Some("minima"), Some(1l)),
        (Some(3), Some(0.5), Some(0.5), None, None),
        (Some(4), Some(0.5), Some(0.0), None, None),
        (Some(5), Some(0.5), Some(0.0), None, None),
        (Some(6), Some(1), Some(0.5), Some("maxima"), Some(2l)),
        (Some(7), Some(0), Some(-1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(1), Some(1), Some("maxima"), Some(0l)),
        (Some(2), Some(0), Some("minima"), Some(1l)),
        (Some(6), Some(1), Some("maxima"), Some(2l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points complex scenario 4") { completeExtremaSetTask =>
      val input = prepareInputDataset(
        (Some(0), Some(0), None, None, None),
        (Some(1), Some(-1), Some(-1.0), Some("maxima"), Some(0l)),
        (Some(2), Some(0), Some(1.0), Some("minima"), Some(1l)),
        (Some(3), Some(-0.5), Some(-0.5), None, None),
        (Some(4), Some(-0.5), Some(0.0), None, None),
        (Some(5), Some(-0.5), Some(0.0), None, None),
        (Some(6), Some(-1), Some(-0.5), Some("maxima"), Some(2l)),
        (Some(7), Some(0), Some(1.0), None, None)
      )
      val expected = prepareOutputDataset(
        (Some(1), Some(-1), Some("maxima"), Some(0l)),
        (Some(2), Some(0), Some("minima"), Some(1l)),
        (Some(6), Some(-1), Some("maxima"), Some(2l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("allExtrema should return all extrema points") { completeExtremaSetTask =>
      // FIXME: change java long to scala
      val input = prepareInputDataset(
        (Some(0.5), Some(0.5), Some(1d), None, None),
        (Some(1), Some(1d), Some(1d), Some("maxima"), Some(0l)),
        (Some(1.5), Some(1d), Some(0d), None, None),
        (Some(2), Some(0.5), Some(-1d), None, None),
        (Some(2.5), Some(0d), Some(-1d), Some("minima"), Some(1l)),
        (Some(3), Some(0d), Some(0d), None, None),
        (Some(3.5), Some(0d), Some(0d), None, None),
        (Some(4), Some(0.5), Some(1d), Some("maxima"), Some(2l)),
        (Some(5), Some(-0.5), Some(-1d), None, None),
        (Some(5.1), Some(-0.5), Some(0d), None, None),
        (Some(5.2), Some(-0.5), Some(0d), None, None),
        (Some(5.5), Some(-1d), Some(-1.6666666666666676), None, None)
      )
      // FIXME: change java long to scala
      val expected = prepareOutputDataset(
        (Some(1), Some(1), Some("maxima"), Some(0l)),
        (Some(1.5), Some(1), Some("maxima"), Some(0l)),
        (Some(2.5), Some(0), Some("minima"), Some(1l)),
        (Some(3), Some(0), Some("minima"), Some(1l)),
        (Some(3.5), Some(0), Some("minima"), Some(1l)),
        (Some(4), Some(0.5), Some("maxima"), Some(2l))
      )

      val result = input.allExtrema("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("findCrossovers should return all crossovers") { completeExtremaSetTask =>
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
        .setNullableForAllColumns(false)
        .setNullable(true, "extrema")

      val result = input.findCrossovers("x", "y")

      assertDataFrameEquals(expected, result)
    }

    // FIXME: pull out tests that test package method
    it("crossoverIndex should return crossover index") { completeExtremaSetTask =>
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
        .setNullableForAllColumns(false)
        .setNullable(true, "extrema")

      val result = input.crossoverIndex("x", "y")

      assertDataFrameEquals(expected, result)
    }
  }

  describe("find differentiation crossover") {
    it("run should produce correct extrema types") { completeExtremaSetTask =>
      val u = udf((i: Int) => i.toLong)
      val diff = loadCsvFile("src/test/resources/data/cartesian_points_diff.csv")
      val reducedExtremaSet = loadCsvFile("src/test/resources/data/cartesian_points_reduced_extrema_set.csv")
        .withColumn("extrema_index", u($"extrema_index"))

      val expected = loadCsvFile("src/test/resources/data/cartesian_points_extrema_set.csv")
        .withColumn("extrema_index", u($"extrema_index"))
        .setNullableForAllColumns(true)

      val completeExtremaSet = completeExtremaSetTask.run(
        Map(
          "diff" -> diff,
          "reducedExtremaSet" -> reducedExtremaSet,
          "xAxisName" -> "x",
          "yAxisName" -> "y"
        )
      ).get


      assertDataFrameEquals(expected, completeExtremaSet)
    }
  }

  private def prepareInputDataset(rows: (Option[Double], Option[Double], Option[java.lang.Double], Option[String], Option[java.lang.Long])*): DataFrame = {
    rows.toDF("x", "y", "diff", "extrema", "extrema_index")
      .withColumn("null_out_x", when($"diff" =!= 0, $"x"))
      .withColumn("start_of_flat_x", last("null_out_x", true).over(Window.orderBy($"x").rowsBetween(unboundedPreceding, currentRow)))
  }

  private def prepareOutputDataset(rows: (Option[Double], Option[Double], Option[String], Option[java.lang.Long])*): DataFrame = {
    rows.toDF("x", "y", "extrema", "extrema_index")
  }
}
