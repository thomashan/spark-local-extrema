package com.github.thomashan.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameUtils {
  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
    })

    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def setNullableState(df: DataFrame, nullable: Boolean, columns: String*): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, n, m) => columns.contains(c) match {
        case true => StructField(c, t, nullable = nullable, m)
        case false => StructField(c, t, n, m)
      }
    })

    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}
