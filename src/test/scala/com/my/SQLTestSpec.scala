package com.my

import org.scalatest._

class SQLTestSpec(deltaTables: List[String]) extends FlatSpec
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with DatabricksTestHelper
  with Matchers
  with GivenWhenThen {

  protected override def beforeAll() : scala.Unit = {
    initSpark()
  }

  protected override def afterAll() : scala.Unit = {
    stopSpark()
  }


  protected override def beforeEach() : scala.Unit = {
    initTables(deltaTables)
  }

  protected override def afterEach() : scala.Unit = {
    cleanTables(deltaTables)
  }
}
