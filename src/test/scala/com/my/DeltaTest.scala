package com.my

import org.scalatest.{GivenWhenThen, Matchers}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

class DeltaTest extends SQLTestSpec(List("test_table")) with GivenWhenThen with Matchers {
  "ISO Date String" should "be parsed to DateTime correctly" in {
    Given("ISO Date String")
    val dateStrRaw = "2015-12-22T08:00Z"

    When("Parsing is done")
    val dateObj = ISODateTimeFormat.dateTimeParser.parseDateTime(dateStrRaw)

    Then("Time is converted to DateTime object correctly")
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC().print(dateObj) should equal("2015-12-22 08:00:00")
  }

  "Delta table " should "be written properly" in {
    Given("Delta table")
    sqlContext.sql("select 1 as id union select 2 as id")
      .write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("overwrite")
      .option("path", s"$getTempDir/tables/mega_table")
      .saveAsTable("mega_table")
    When("Table is updated")
  //  sqlContext.sql("DELETE FROM mega_table where id = 1")
    sqlContext.sql("insert into mega_table select 3 as id")

    Then("the table was updated on disk")

    sqlContext.sql("select sum(id) as id_sum from mega_table").collect().head.getAs[Int](0) should equal(1 + 2 + 3)
  }

  "Delta table " should "be created during initialization" in {
    Given("Delta table")
    val initValues = List(121, 122)
    When("Table has being read")
    val result = sqlContext.sql("select id from test_table").collect().map(_.getAs[Int](0))
    Then("table can be read")
    result should have size 2
    result.sortWith(_ < _) should equal(initValues)
  }

}
