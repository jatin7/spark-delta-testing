package com.my

import java.util.ServiceConfigurationError

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MyTest {

  private def outDF(dataframe: DataFrame, description: String) = {
    println(s"Start dataframe $description ------------------")
    dataframe.show()
    println(s"End dataframe $description --------------------")
    dataframe
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      throw new ServiceConfigurationError(
        s"""Incorrect parameters number (passed ${args.length}, expected 2):
          |   CSV_source_path
          |   conversion_configuration_path
          |   """.stripMargin)
    }
    val csvPath = args(0)
    val jsonPath = args(1)

    val conf = new SparkConf(true).setAppName("TestApp")
  //    .setMaster("local[*]") //Uncomment this for local run
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val csvProcessor = CSVProcessor(spark)
    val jsonProcessor = JsonProcessor(spark)
    val dataset = csvProcessor.read(csvPath)
    val filteredDataset = outDF(csvProcessor.filterEmptyValues(dataset), "filteredDataset")
    val configurationJson = outDF(jsonProcessor.read(jsonPath), "Mapping configuration")
    val conversionSQL = jsonProcessor.convertToSQL(configurationJson)
    val convertedDataset = outDF(csvProcessor.convertDataTypes(filteredDataset, conversionSQL), "converted DF")
    val dataProfile = csvProcessor.dataProfilingSimplest(convertedDataset)
    println("start data profile-------------")
    println(dataProfile)
    println("end data profile---------------")
  }
}
