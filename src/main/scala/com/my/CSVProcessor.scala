package com.my

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{IntegerType, LongType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class CSVProcessor(@transient spark: SparkSession) {
  def read(dataPath: String): DataFrame = {
    spark.read.option("header", "true").option("quote", "'").csv(dataPath)
  }

  def filterEmptyValues(input: DataFrame): DataFrame = {
    val stringFields = input.schema.fields.filter(_.dataType == StringType).map(_.name)
    input.filter { arow =>
      !arow.getValuesMap[String](stringFields).exists { value =>
        value._2 != null && value._2.trim.equals("")
      }
    }
  }

  def convertDataTypes(inputDataFrame: DataFrame, sql: String): DataFrame = {
    inputDataFrame.createOrReplaceTempView("justView")
    spark.sql(s"select $sql from justView")
  }

  def dataProfilingSimplest(inputDataFrame: DataFrame): String = {
    val listOfData = inputDataFrame.columns.map { column =>
      val values = inputDataFrame
        .groupBy(column)
        .count()
        .filter(s"$column is not null")
        .withColumn(column, expr(s"cast($column as string)"))
        .collect()
        .map(row => row.getAs[String](column) -> row.getAs[Long]("count")).toMap

      Row(column, values.size, values)
    }
    listOfData.foreach(println)
    val struct = new StructType()
      .add("Column",StringType)
      .add("Unique_values", IntegerType)
      .add("Values", MapType(StringType,LongType))
    spark.createDataFrame(spark.sparkContext.parallelize(listOfData), struct).toJSON.collect().mkString
  }
}
