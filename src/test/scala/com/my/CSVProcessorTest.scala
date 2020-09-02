package com.my

import org.scalatest.{GivenWhenThen, Matchers}

class CSVProcessorTest extends SparkTestSpec with GivenWhenThen with Matchers {

  "CSV Processor" should "be read file properly" in {
    Given("CSV processor")
    val processor = CSVProcessor(spark)

    When("It has been read")
    val result = processor.read("./src/test/resources/sample.csv")

    Then("")
    val a = result.count()
    a should equal(7L)
  }

}
