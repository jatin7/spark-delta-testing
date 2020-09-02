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

}
