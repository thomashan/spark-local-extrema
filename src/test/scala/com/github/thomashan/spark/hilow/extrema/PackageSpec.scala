package com.github.thomashan.spark.hilow.extrema

import com.github.thomashan.spark.{File, SparkSpec}
import org.scalatest.FunSpec

class PackageSpec extends FunSpec with SparkSpec {
  describe("implementation details") {
    describe("removeDuplicate") {
      it("should remove duplicates extrema") {
        val input = File.loadCsv("src/test/resources/data/hi_low_diff.csv")
      }
    }

    describe("removeUnusedExtrema") {
      it("should remove unused extrema") {

      }
    }

    describe("trueExtremaInPartition") {
      it("should get true extrema in partition") {

      }
    }
  }
}
