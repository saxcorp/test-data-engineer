package com.littlebigcode.spark.TestDataEngineer

import com.littlebigcode.spark.TestDataEngineer.common.Tools.getGlobalConfig
import com.littlebigcode.spark.TestDataEngineer.core.{InputArgs, InputArgsBuilder}
import com.littlebigcode.spark.TestDataEngineer.core.exercise.Exercise01
import com.littlebigcode.spark.TestDataEngineer.core.exercise.Exercise02.{produceExercise2}
import com.littlebigcode.spark.TestDataEngineer.core.exercise.Exercise03.{produceExercise3a, produceExercise3b}
import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {
    val inputArgs = InputArgsBuilder.build(args)
    val globalConfig = getGlobalConfig()

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName(s"LBC-AppExercise-CodingGame-${globalConfig.env}")
      .getOrCreate()

    runApp(spark, globalConfig, inputArgs)
  }

  /**
   * Orchestre la production des 4 sources différentes sources de données (''exercise01/exercise02/exercise03a/exercise03b'').
   * Une exception est levé lorsque la valeur du paramètre de ligne de commande "''--produce'''" est différente des 4 valeurs possibles.
   *
   * @param spark Session spark
   * @param appGlobalConfig La configuration globale chargé depuis le fichier de configuration  '''application.conf'''.
   * @param inputArgs [[com.littlebigcode.spark.TestDataEngineer.core.InputArgs]] L'argument d'entré de ligne de commande qui permet d'identifier la source de donnée à produire.
   */
  def runApp(spark: SparkSession, appGlobalConfig: AppGlobalConfig, inputArgs: InputArgs): Unit = {
    inputArgs.dataProductionType match {
      case "exercise01" => Exercise01.produceExercise1(spark, appGlobalConfig)
      case _ =>
        val dfCampagne = spark.read.parquet(appGlobalConfig.hdfsExercice0203InputCampagneParquetPath)
        inputArgs.dataProductionType match {
          case "exercise02" => produceExercise2(appGlobalConfig, dfCampagne)
          case "exercise03a" => produceExercise3a(appGlobalConfig,
            produceExercise2(appGlobalConfig, dfCampagne, false)
          )
          case "exercise03b" => produceExercise3b(appGlobalConfig, dfCampagne)
          case _ => new IllegalArgumentException(s"Unable to found dataProductionType : \n${InputArgsBuilder.usage}")
        }
    }
  }
}
