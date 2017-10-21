package com.github.thomashan.spark

import org.apache.spark.sql.SparkSession

abstract class SparkJob {
  val applicationName: String
  lazy implicit val spark = SparkSession
    .builder
    .config("spark.sql.orc.filterPushdown", "true")
    .appName(applicationName)
    .getOrCreate()

  protected def run(args: Array[String])
}

