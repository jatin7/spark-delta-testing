package com.my

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import java.nio.file.Paths

trait AbstractSparkTestsHelper {

  protected def mutateSparkConfig(sparkConf: SparkConf): SparkConf = sparkConf

  protected val sparkConfig: SparkConf = mutateSparkConfig(new SparkConf()
    .set("spark.broadcast.compress", "false")
    .set("spark.sql.catalogImplementation", "hive")
    .setMaster("local[*]")
    .set("spark.local.dir", s"$getTempDir")
    .setAppName(this.getClass.getName + "TestApp")
    .set("spark.app.id", "local-1231231231")
    .set("spark.ui.enabled", "false"))

  protected implicit var sc: SparkContext = _

  protected def initSpark(): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc = new SparkContext(sparkConfig)
    Logger.getRootLogger.setLevel(Level.ERROR)
  }

  protected def afterStopSpark(): Unit = {}

  protected def stopSpark(): Unit = {
    try {
      Logger.getRootLogger.setLevel(Level.ERROR)
      sc.stop()
      afterStopSpark()
    } catch {
      case e: Throwable =>
    }

    Logger.getRootLogger.setLevel(Level.ERROR)
  }

  protected def getTempDir: String = s"${Paths.get(".").toAbsolutePath}/target"
}
