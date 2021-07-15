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
   *
   * @param spark
   * @param appGlobalConfig
   * @param inputArgs
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
