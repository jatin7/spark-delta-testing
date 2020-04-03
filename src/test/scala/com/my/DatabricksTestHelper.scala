package com.my


import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory
import scala.util.Try

trait DatabricksTestHelper extends AbstractSparkTestsHelper {

  private def hivePath = new File(s"$getTempDir/hive").getAbsolutePath
  private def hiveMetastoreDBLocation = s"$getTempDir/metastore_db_for_tests${System.currentTimeMillis()}"

  override def mutateSparkConfig(sparkConf: SparkConf): SparkConf = {
    super.mutateSparkConfig(sparkConf)
      .set("spark.sql.warehouse.dir", hivePath)
      .set("spark.sql.catalogImplementation", "hive")
      .set("hive.metastore.warehouse.dir", hivePath)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveMetastoreDBLocation;create=true")
      .set("hive.exec.scratchdir", s"$getTempDir/hiveScratchDir")
  }

  protected def sqlContext: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

  protected def initTables(deltaTables: List[String]): Unit = {
    initDatabases(deltaTables)

    for {
      tableDirectory <- new File("./src/test/resources/sql").listFiles.toIterator
      tableName = tableDirectory.getName
      if tableDirectory.isDirectory && deltaTables.contains(tableName)
      file <- tableDirectory.listFiles.toIterator
      commandArr = file.getName.split("\\.").filter(command => Set("create", "insert").contains(command))
    } {
      val command = commandArr.headOption.getOrElse(throw new RuntimeException(s"There is no command in file ${file.getName}"))

      val bufferedSource = scala.io.Source.fromFile(file)

      Try(bufferedSource.mkString).map(rawScript => {
        val scripts = command match {
          case "create" => List(s"create table if not exists $tableName ( \n$rawScript \n)", s"truncate table $tableName")
          case _ => List(s"insert into $tableName select $rawScript")
        }
        scripts.foreach(script => sqlContext.sql(script))
      }).recover{ case a: Throwable =>
        println(s"Can't read a script: ${file.getCanonicalPath}")
        println(s"Error is ${a.getMessage}")
      }

      bufferedSource.close()
    }
  }

  private def initDatabases(deltaTables: List[String]): Unit = {
    deltaTables.filter(
      _.contains(".")
    ).flatMap(
      fieldName =>
        fieldName.split("\\.").headOption
    ).distinct
      .foreach(databaseName =>
        sqlContext.sql(s"create database if not exists $databaseName")
      )
  }

  protected def cleanTables(deltaTables: List[String]): Unit = {
    for {
      table <- deltaTables
    } {
      val script = s"drop table $table"
      sqlContext.sql(script)
    }
  }

  override def afterStopSpark(): Unit = {
    List(hivePath, hiveMetastoreDBLocation)
      .map(dirName => new Directory(new File(dirName)))
      .foreach(_.deleteRecursively)
  }
}
