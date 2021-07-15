package com.littlebigcode.spark.TestDataEngineer.core

import scopt.OParser

case class InputArgs(dataProductionType: String = "")

object InputArgsBuilder {
  /**
   *
   * @param args
   * @return
   */
  def build(args: Array[String]) = {

    OParser.parse(getInputArgsParser, args, InputArgs()) match {
      case Some(inputargs) => inputargs
      case _ =>
        throw new IllegalArgumentException(s"Unparsable input : \n${usage}")
    }
  }

  val usage = {
    """
      |
      |""".stripMargin
  }

  /**
   *
   * @return
   */
  private def getInputArgsParser = {
    val builder = OParser.builder[InputArgs]
    val appParser = {
      import builder._
      OParser.sequence(
        programName("LBC - TestDataEngineer"),
        head("scopt", "4.x"),

        opt[String]('p', "produce")
          .action((x, c) => c.copy(dataProductionType = x))
          .text("Name of target data to produce.")
      )
    }
    appParser
  }
}
