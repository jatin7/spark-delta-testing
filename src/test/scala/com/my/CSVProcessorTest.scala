package com.my

import org.scalatest.{GivenWhenThen, Matchers}

class CSVProcessorTest extends SparkTestSpec with GivenWhenThen with Matchers {

  "CSV Processor" should "read file properly" in {
    Given("CSV processor")
    val processor = CSVProcessor(spark)

    When("It has been read")
    val result = processor.read("./src/test/resources/sample.csv")

    Then("should return expected row count")
    result.show()
    result.count() should equal(7L)
  }

  "CSV Processor" should "filter empty rows out" in {
    Given("CSV processor")
    val processor = CSVProcessor(spark)
    And("dataset")
    val datasets =
      Map("select 'a' as value union select '' as value union select null as value" -> 2,
        "select 'a' as value, '' as value2" -> 0)

    When("datasets are filtered")

    Then("should return expected row count")
    datasets.foreach(a => processor.filterEmptyValues(spark.sql(a._1)).count should equal(a._2))

  }

}
