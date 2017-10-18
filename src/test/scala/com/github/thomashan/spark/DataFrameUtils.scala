package com.github.thomashan.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameUtils {
  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean): DataFrame = {
    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) ⇒ StructField(c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }
}
