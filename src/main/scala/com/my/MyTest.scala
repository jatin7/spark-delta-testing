package com.my

import org.apache.spark.sql.SparkSession

object MyTest {
  def main(args: Array[String]): Unit = {
    //println("All interesting is in test folder")
   val spark = SparkSession.builder().getOrCreate()
  }
}
