package com.github.thomashan.spark

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

trait SparkSpec extends DatasetSuiteBase with Suite with BeforeAndAfter with BeforeAndAfterAll {
  override lazy implicit val spark = SparkSessionProvider._sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .sparkContext
      .setLogLevel("WARN")
  }

  protected def loadCsvFile(path: String): DataFrame = {
    spark.read.option("header", true).option("inferSchema", true).csv(path)
  }
}
