package com.littlebigcode.spark.TestDataEngineer.common

import com.littlebigcode.spark.TestDataEngineer.common.Tools.getConfigStringOrElse
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ToolsTests extends AnyFunSuite {
  test("getStructTypeFromClassPathJson()") {
    val res = Tools.getStructTypeFromClassPathJson("testSchema.json")
    val expectString = "StructType(StructField(field1,StringType,true), StructField(field2,IntegerType,true), StructField(field3,LongType,true), StructField(field4,FloatType,true))"
    assert(expectString.equals(res.toString()))
  }
  test("getConfigStringOrElse()") {
    val conf =
      """
        |{
        |test.property1=toto
        |test.property2=tata
        |}
        |""".stripMargin

    val c = ConfigFactory.parseString(conf)

    assert(getConfigStringOrElse(c, "test.property1").equals("toto"))
    assert(getConfigStringOrElse(c, "test.property2", "NA").equals("tata"))
    assert(getConfigStringOrElse(c, "test.property3", "DefaultVal").equals("DefaultVal"))
  }
}
