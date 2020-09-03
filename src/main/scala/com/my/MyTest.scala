package com.my

import org.apache.spark.sql.SparkSession

object MyTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val csvProcessor = CSVProcessor(spark)
    val jsonProcessor = JsonProcessor(spark)
  }
}
