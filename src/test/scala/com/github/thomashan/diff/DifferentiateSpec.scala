package com.github.thomashan.diff

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSpec}

class DifferentiateSpec extends FunSpec with DatasetSuiteBase with BeforeAndAfter {
  var cartesianPoints: DataFrame = _

  before {
    cartesianPoints = spark.read.option("header", true).csv("src/test/resources/data/cartesian_points.csv")
  }

  describe("test data") {
    it("should be able to read it") {
      cartesianPoints.collect().foreach(row => println(row))
    }
  }
}
