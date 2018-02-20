package com.github.thomashan.spark

import com.github.thomashan.spark.common.LoadCsvFileTask
import org.apache.spark.sql.DataFrame

object File {
  def loadCsv(csvFile: String)(implicit spark: org.apache.spark.sql.SparkSession): DataFrame = {
    new LoadCsvFileTask().run(Map(
      "inputFile" -> csvFile,
      "header" -> true
    )).get.orderBy("x")
  }
}
