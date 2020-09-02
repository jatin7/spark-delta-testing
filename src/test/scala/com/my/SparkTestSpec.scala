package com.my

import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkTestSpec() extends FlatSpec
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with AbstractSparkTestsHelper
  with Matchers
  with GivenWhenThen {

  var sparkInternal: Option[SparkSession] = None

  def spark: SparkSession = sparkInternal match {
    case Some(value) => value
    case _ =>  throw new RuntimeException("spark context for tests is not initialized")
  }

  protected override def beforeAll() : scala.Unit = {
    initSpark()
    sparkInternal = Some(SparkSession.builder().getOrCreate())
  }

  protected override def afterAll() : scala.Unit = {
    stopSpark()
  }
}
