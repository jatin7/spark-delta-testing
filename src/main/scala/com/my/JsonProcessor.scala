package com.my

import org.apache.spark.sql.{DataFrame, SparkSession}

case class JsonProcessor(@transient spark: SparkSession) {
  def read(dataPath: String): DataFrame = {
    spark.read.option("multiLine", "true").json(dataPath)
  }

  def convertToSQL(convertDefinition: DataFrame): String = {
    convertDefinition.collect().map{ arow =>
      if (arow.getAs[String]("new_data_type") == "date") {
        s"TO_DATE(CAST(UNIX_TIMESTAMP(${arow.getAs[String]("existing_col_name")}, '${arow.getAs[String]("date_expression")}') AS TIMESTAMP)) as ${arow.getAs[String]("new_col_name")}"
      } else {
        s"cast(${arow.getAs[String]("existing_col_name")} as ${arow.getAs[String]("new_data_type")}) as ${arow.getAs[String]("new_col_name")}"
      }
    }.mkString(", ")
  }

}
