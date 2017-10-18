package com.github.thomashan.spark

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSpec}

abstract class SparkSpec extends FunSpec with DatasetSuiteBase with BeforeAndAfter {
  override lazy implicit val spark = SparkSessionProvider._sparkSession

  protected def loadCsvFile(path: String): DataFrame = {
    spark.read.option("header", true).option("inferSchema", true).csv(path)
  }
}
