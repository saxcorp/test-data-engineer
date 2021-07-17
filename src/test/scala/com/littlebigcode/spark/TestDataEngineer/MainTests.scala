package com.littlebigcode.spark.TestDataEngineer

import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import com.github.mrpowers.spark.fast.tests.DatasetComparer

@RunWith(classOf[JUnitRunner])
class MainTests extends AnyFunSuite with SharedSparkContext with DatasetComparer {
  cleanDataDir()

  test("produce exercise01") {
    val args = List("--produce", "exercise01")
    runAppTestMode(args)

    val res = sparkSession.read.parquet("outputs/exercise-01")
    val expected = sparkSession.read.parquet("src/test/resources/expected/exercise-01")
    assertSmallDatasetEquality(res, expected)
  }

  test("produce exercise02") {
    val args = List("--produce", "exercise02")
    runAppTestMode(args)

    val res = sparkSession.read.parquet("outputs/exercise-02")
    val expected = sparkSession.read.parquet("src/test/resources/expected/exercise-02")
    assertSmallDatasetEquality(res, expected)
  }

  test("produce exercise03a") {
    val args = List("--produce", "exercise03a")
    runAppTestMode(args)

    val res = sparkSession.read.parquet("outputs/exercise-03/exercise03a")
    val expected = sparkSession.read.parquet("src/test/resources/expected/exercise-03/exercise03a")
    assertSmallDatasetEquality(res, expected)
  }

  test("produce exercise03b") {
    val args = List("--produce", "exercise03b")
    runAppTestMode(args)

    val res = sparkSession.read.parquet("outputs/exercise-03/exercise03b")
    val expected = sparkSession.read.parquet("src/test/resources/expected/exercise-03/exercise03b")
    assertSmallDatasetEquality(res, expected)
  }
}
