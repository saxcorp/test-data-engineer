package com.littlebigcode.spark.TestDataEngineer

import com.littlebigcode.spark.TestDataEngineer.common.Tools.getGlobalConfig
import com.littlebigcode.spark.TestDataEngineer.core.{InputArgs, InputArgsBuilder}
import com.littlebigcode.spark.TestDataEngineer.core.exercises.Exercice1
import com.littlebigcode.spark.TestDataEngineer.core.exercises.Exercice2.{Ex2MonthDeadlineFilterType, produceExercise2}
import com.littlebigcode.spark.TestDataEngineer.core.exercises.Exercice3.{produceExercise3a, produceExercise3b}
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

  def runApp(spark: SparkSession, appGlobalConfig: AppGlobalConfig, inputArgs: InputArgs): Unit = {
    inputArgs.dataProductionType match {
      case "exercice1" => Exercice1.produceExercise1(spark, appGlobalConfig)
      case _ =>
        val dfCampagne = spark.read.parquet(appGlobalConfig.hdfsExercice0203InputCampagneParquetPath)
        inputArgs.dataProductionType match {
          case "exercise2v1_FilterType_EQUAL" => produceExercise2(appGlobalConfig, dfCampagne, Ex2MonthDeadlineFilterType.EQUAL)
          case "exercise2v2_FilterType_SUPERIOR" => produceExercise2(appGlobalConfig, dfCampagne, Ex2MonthDeadlineFilterType.SUPERIOR)
          case "exercise2v3_FilterType_EQUAL_OR_SUPERIOR" => produceExercise2(appGlobalConfig, dfCampagne, Ex2MonthDeadlineFilterType.EQUAL_OR_SUPERIOR)
          case "exercise3v1a" => produceExercise3a(appGlobalConfig,
            produceExercise2(appGlobalConfig, dfCampagne, Ex2MonthDeadlineFilterType.EQUAL_OR_SUPERIOR, false)
          )
          case "exercise3v1b" => produceExercise3b(appGlobalConfig, dfCampagne)
          case _ => new IllegalArgumentException(s"Unable to found dataProductionType : \n${InputArgsBuilder.usage}")
        }
    }
  }
}
