package com.github.thomashan.spark

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.DataFrame

package object diff {

  implicit private[diff] class Methods(dataFrame: DataFrame) {

    import dataFrame.sqlContext.implicits._

    def diff(xAxisName: String, yAxisName: String): DataFrame = {
      dataFrame
        .rdd
        .sliding(2)
        .map { array =>
          val element0 = array.head
          val element1 = array.last
          val x0 = element0.getDouble(0)
          val x1 = element1.getDouble(0)
          val y0 = element0.getDouble(1)
          val y1 = element1.getDouble(1)
          val diff = (y1 - y0) / (x1 - x0)

          (x1, y1, diff)
        }
        .toDF(xAxisName, yAxisName, "diff")
    }
  }

}
