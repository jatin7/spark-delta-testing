package com.my

import org.scalatest.{GivenWhenThen, Matchers}

class GlobalIT extends  SparkTestSpec with GivenWhenThen with Matchers {
  "All pipeline" should "process the data as in the specification" in {
    Given("CSV and JSON processors")
    val jsonProcessor = JsonProcessor(spark)
    val csvProcessor = CSVProcessor(spark)

    When("all files read and processed")
    val fields_mapping = jsonProcessor.read("./src/test/resources/sample.json")
    val dataset = csvProcessor.read("./src/test/resources/sample.csv")
    val filteredValues = csvProcessor.filterEmptyValues(dataset)
    val fieldsDefinition = jsonProcessor.convertToSQL(fields_mapping)
    filteredValues.show()
    println(fieldsDefinition)
    val result = csvProcessor.convertDataTypes(filteredValues, fieldsDefinition)

    val dataProfile = csvProcessor.dataProfilingSimplest(result)

    Then("should return expected row count")
    result.show()
    result.count() should equal(3L)
    println(dataProfile)
  }
}
