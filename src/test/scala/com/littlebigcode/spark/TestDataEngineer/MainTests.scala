package com.littlebigcode.spark.TestDataEngineer

import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainTests extends AnyFunSuite with SharedSparkContext {
  cleanDataDir()

  test("produce exercise01") {
    val args = List("--produce", "exercise01")
    runAppTestMode(args)
  }

  test("produce exercise02") {
    val args = List("--produce", "exercise02")
    runAppTestMode(args)
  }

  test("produce exercise03a") {
    val args = List("--produce", "exercise03a")
    runAppTestMode(args)
  }

  test("produce exercise03b") {
    val args = List("--produce", "exercise03b")
    runAppTestMode(args)
  }
}
