package com.littlebigcode.spark.TestDataEngineer

import com.littlebigcode.spark.TestDataEngineer.Main.runApp
import com.littlebigcode.spark.TestDataEngineer.common.Tools.getGlobalConfig
import com.littlebigcode.spark.TestDataEngineer.core.InputArgsBuilder
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import java.io.File
import scala.reflect.io.Directory

@RunWith(classOf[JUnitRunner])
trait SharedSparkContext {
  val sparkSession = SparkSession
    .builder()
    .appName("SparkSession for unit tests")
    .config("spark.sql.shuffle.partitions", "1")
    .master("local[*]")
    .getOrCreate()

  def runAppTestMode(args: List[String]) = {
    val inputArgs = InputArgsBuilder.build(args.toArray)
    val globalConfig = getGlobalConfig()
    runApp(sparkSession, globalConfig, inputArgs)
  }

  def cleanDataDir()={
    val directory = new Directory(new File("outputs"))
    directory.deleteRecursively()
  }
}
