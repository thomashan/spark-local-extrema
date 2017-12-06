package com.github.thomashan

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

package object spark {

  implicit class SchemaMethods(val dataFrame: DataFrame) {
    def setNullableForAllColumns(nullable: Boolean): DataFrame = {
      val schema = dataFrame.schema
      val newSchema = StructType(schema.map {
        case StructField(c, t, _, m) => StructField(c, t, nullable, m)
      })

      dataFrame.sqlContext.createDataFrame(dataFrame.rdd, newSchema)
    }

    def setNullable(nullable: Boolean, columns: String*): DataFrame = {
      val schema = dataFrame.schema
      val newSchema = StructType(schema.map {
        case StructField(c, t, n, m) => columns.contains(c) match {
          case true => StructField(c, t, nullable, m)
          case false => StructField(c, t, n, m)
        }
      })

      dataFrame.sqlContext.createDataFrame(dataFrame.rdd, newSchema)
    }
  }

}
