package com.my

import org.apache.spark.sql.{DataFrame, SparkSession}

case class CSVProcessor(@transient spark: SparkSession) {
  def read(dataPath: String): DataFrame = {
    spark.read.csv(dataPath)
  }
}
