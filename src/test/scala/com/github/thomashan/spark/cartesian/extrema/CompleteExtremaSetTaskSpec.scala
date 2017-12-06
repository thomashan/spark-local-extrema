package com.github.thomashan.spark.cartesian.extrema

import com.github.thomashan.spark.{SparkSpec, _}
import org.apache.spark.sql.functions._
import org.scalatest.{Outcome, fixture}

class CompleteExtremaSetTaskSpec extends fixture.FunSpec with SparkSpec {

  import spark.implicits._

  type FixtureParam = CompleteExtremaSetTask

  override def withFixture(test: OneArgTest): Outcome = {
    val completeExtremaSetTask = new CompleteExtremaSetTask()
    test(completeExtremaSetTask)
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
}
