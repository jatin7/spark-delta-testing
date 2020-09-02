package com.my

import org.scalatest.{GivenWhenThen, Matchers}

class JsonProcessorTest extends  SparkTestSpec with GivenWhenThen with Matchers {
  "Json Processor" should "read file properly" in {
    Given("Json processor")
    val processor = JsonProcessor(spark)

    When("It has been read")
    val result = processor.read("./src/test/resources/sample.json")

    Then("should return expected row count")
    result.show()
    result.count() should equal(3L)
  }

  "Json Processor" should "generate SQL" in {
    Given("Json processor")
    val processor = JsonProcessor(spark)

    When("fields definition")
    val definition = spark.sql(
      """select 'name' as existing_col_name,
        |'first_name' as new_col_name,
        |'string' as new_data_type
        |""".stripMargin)

    Then("should return expected SQL")
    processor.convertToSQL(definition) should equal("cast(name as string) as first_name")

    When("fields definition with date")
    val definition2 = spark.sql(
      """select 'name' as existing_col_name,
        |'first_name' as new_col_name,
        |'date' as new_data_type,
        |'dd-MM-yyyy' as date_expression
        |""".stripMargin)

    Then("should return expected SQL")
    processor.convertToSQL(definition2) should equal("TO_DATE(CAST(UNIX_TIMESTAMP(name, 'dd-MM-yyyy') AS TIMESTAMP)) as first_name")
  }

}
