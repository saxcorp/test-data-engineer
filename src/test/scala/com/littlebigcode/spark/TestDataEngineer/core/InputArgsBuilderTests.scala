package com.littlebigcode.spark.TestDataEngineer.core

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InputArgsBuilderTests extends AnyFunSuite{
  test("build()"){
    val res1 = InputArgsBuilder.build(List("--produce", "exercise01").toArray)
    assert(res1.isInstanceOf[InputArgs])
    assert("InputArgs(exercise01)".equals(res1.toString))

    val res2 = InputArgsBuilder.build(List("--produce", "exercise02").toArray)
    assert(res2.isInstanceOf[InputArgs])
    assert("InputArgs(exercise02)".equals(res2.toString))

    val res3 = InputArgsBuilder.build(List("--produce", "exercise03a").toArray)
    assert(res3.isInstanceOf[InputArgs])
    assert("InputArgs(exercise03a)".equals(res3.toString))

    val res4 = InputArgsBuilder.build(List("--produce", "exercise03b").toArray)
    assert(res4.isInstanceOf[InputArgs])
    assert("InputArgs(exercise03b)".equals(res4.toString))

    assertThrows[java.lang.IllegalArgumentException] {
      InputArgsBuilder.build(List("--produce", "titi").toArray)
    }

    assertThrows[java.lang.IllegalArgumentException] {
      InputArgsBuilder.build(List("--produce").toArray)
    }

    assertThrows[java.lang.IllegalArgumentException] {
      InputArgsBuilder.build(List().toArray)
    }
  }
}
