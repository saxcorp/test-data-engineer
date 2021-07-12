package com.littlebigcode.spark.TestDataEngineer

import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainTests extends AnyFunSuite with SharedSparkContext {
  test("produce exercice1") {
    val args = List("--produce", "exercice1")
    runAppTestMode(args)
  }

  test("produce exercise2v1_FilterType_EQUAL") {
    val args = List("--produce", "exercise2v1_FilterType_EQUAL")
    runAppTestMode(args)
  }

  test("produce exercise2v2_FilterType_SUPERIOR") {
    val args = List("--produce", "exercise2v2_FilterType_SUPERIOR")
    runAppTestMode(args)
  }

  test("produce exercise2v3_FilterType_EQUAL_OR_SUPERIOR") {
    val args = List("--produce", "exercise2v3_FilterType_EQUAL_OR_SUPERIOR")
    runAppTestMode(args)
  }

  test("produce exercise3v1a") {
    val args = List("--produce", "exercise3v1a")
    runAppTestMode(args)
  }

  test("produce exercise3v1b") {
    val args = List("--produce", "exercise3v1b")
    runAppTestMode(args)
  }
}
