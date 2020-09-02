package com.my

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CSVProcessor(@transient spark: SparkSession) {
  def read(dataPath: String): DataFrame = {
    spark.read.csv(dataPath)
  }

  def filterEmptyValues(input: DataFrame): DataFrame = {
    val stringFields = input.schema.fields.filter(_.dataType == StringType).map(_.name)
    input.filter { arow =>
      !arow.getValuesMap[String](stringFields).exists { value =>
        value._2 != null && value._2.trim.equals("")
      }
    }
  }
}
