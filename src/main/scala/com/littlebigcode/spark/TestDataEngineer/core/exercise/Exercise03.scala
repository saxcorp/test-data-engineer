package com.littlebigcode.spark.TestDataEngineer.core.exercise

import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object Exercise03 {
  /**
   * Produit les données de l'exercice 03a et les sauvegarde en parquet. Les données sauvegardées sont retournées dans un DataFrame.
   *
   * @param appGlobalConfig Configuration globale de l'application provenant du fichier application.conf.
   * @param dfExercise2 Dataframe des données provenant de l'exrcice 02.
   * @return '''org.apache.spark.sql.DataFrame'''
   */
  def produceExercise3a(appGlobalConfig: AppGlobalConfig, dfExercise2: DataFrame) = {
    val res = dfExercise2.orderBy(col("TOTAL_CLIENTS").desc).limit(3)

    res.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${appGlobalConfig.hdfsExercice03OutputDir}/exercise03a")

    res
  }

  /**
   * Produit les données de l'exercice 03b et les sauvegarde en parquet. Les données sauvegardées sont retournées dans un DataFrame.
   *
   * @param appGlobalConfig Configuration globale de l'application provenant du fichier application.conf.
   * @param dfExercise2 Dataframe des données produites par l'exrcice 02.
   * @return '''org.apache.spark.sql.DataFrame'''
   */
  def produceExercise3b(appGlobalConfig: AppGlobalConfig, dfExercise2: DataFrame) = {
    val windowSpec = Window.partitionBy("NMPTF").orderBy(col("NB_CONTRATS").desc)

    val res = dfExercise2
      .select("NMPTF", "00021_NUMCLIA", "00004_NUMCLE")
      .groupBy("NMPTF", "00021_NUMCLIA").count().withColumnRenamed("count", "NB_CONTRATS")
      .withColumn("CLASSEMENT", row_number().over(windowSpec)).where("CLASSEMENT <= 3")

    res.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${appGlobalConfig.hdfsExercice03OutputDir}/exercise03b")

    res
  }
}
