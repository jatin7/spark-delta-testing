package com.my

import org.apache.spark.sql.{DataFrame, SparkSession}

case class JsonProcessor(@transient spark: SparkSession) {
  def read(dataPath: String): DataFrame = {
    spark.read.option("multiLine", "true").json(dataPath)
  }
}
