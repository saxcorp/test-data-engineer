package com.littlebigcode.spark.TestDataEngineer.core

import scopt.OParser

/**
 *
 * @param dataProductionType Type de données à produire. Dois prendre seulement l'une des valeur suivantes ''exercise01/exercise02/exercise03a/exercise03b''.
 */
case class InputArgs(dataProductionType: String = "")

object InputArgsBuilder {
  /**
   * Parse les arguments d'entrées en ligne de commande pour construire [[com.littlebigcode.spark.TestDataEngineer.core.InputArgs]].
   * Une exception est levée lorsque les arguments d'entrés ne sont pas conformes.
   *
   * @param args Liste de chaine de caractère des arguments d'entré de la ligne de commande.
   * @return [[com.littlebigcode.spark.TestDataEngineer.core.InputArgs]]
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
      |Usage : ./spark-submit.sh --produce [DATA_TYPE_PRODUCE]
      |  With << --produce [DATA_TYPE_PRODUCE]" >> is mandatory.
      |
      | Examples :
      |    ./spark-submit.sh --produce exercise01
      |    ./spark-submit.sh --produce exercise02
      |    ./spark-submit.sh --produce exercise03a
      |    ./spark-submit.sh --produce exercise03b
      |
      |""".stripMargin
  }

  /**
   * Le parseur de l'entrée de commande.
   *
   * @return Le parseur
   */
  private def getInputArgsParser = {
    val builder = OParser.builder[InputArgs]
    val appParser = {
      import builder._
      OParser.sequence(
        programName("LBC - TestDataEngineer"),
        head("scopt", "4.x"),

        opt[String]('p', "produce")
          .required()
          .action((x, c) => c.copy(dataProductionType = x))
          .validate(x =>
            List("exercise01", "exercise02", "exercise03a", "exercise03b").contains(x) match {
              case true => success
              case _ => failure("Seul les valeurs suivantes sont autorisées : \"exercise01\", \"exercise02\", \"exercise03a\", \"exercise03b\"")
            }
          )
          .text("Name of target data to produce. (\"exercise01\", \"exercise02\", \"exercise03a\", \"exercise03b\")")
      )
    }
    appParser
  }
}
