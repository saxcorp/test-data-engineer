package com.littlebigcode.spark.TestDataEngineer.core.exercise

import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

object Exercise02 {

  /**
   * Produit les données de l'exercice 02 et les sauvegarde en parquet. Les données sauvegardées sont retournées dans un DataFrame.
   *
   * @param appGlobalConfig Configuration globale de l'application provenant du fichier application.conf.
   * @param dfCampagne Dataframe contenant les données de la compagne.
   * @param saveResult Si True active la sauvegarde des données produites en parquet. Si non aucune sauvegarde n'est faite.
   * @return '''org.apache.spark.sql.DataFrame'''
   */
  def produceExercise2(appGlobalConfig: AppGlobalConfig,
                       dfCampagne: DataFrame,
                       saveResult: Boolean = true) = {
    val dff1 = dfCampagne.select("NMPTF", "00021_NUMCLIA").distinct()
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "TOTAL_CLIENTS")

    val dff2 = dfCampagne.select("NMPTF", "00004_NUMCLE").distinct()
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "TOTAL_CONTRATS")

    val dff3 = dfCampagne.where(s"01255_MOISAN = 1").select("NMPTF", "00004_NUMCLE")
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "ECH_1")

    val dff4 = dfCampagne.where(s"01255_MOISAN = 2").select("NMPTF", "00004_NUMCLE")
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "ECH_2")

    val dff5 = dfCampagne.where(s"01255_MOISAN = 11").select("NMPTF", "00004_NUMCLE")
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "ECH_3")

    val dff6 = dfCampagne.where(s"01255_MOISAN = 12").select("NMPTF", "00004_NUMCLE")
      .groupBy("NMPTF").count()
      .withColumnRenamed("count", "ECH4")

    val finalDf = dff1
      .join(dff2, Seq("NMPTF"), "left")
      .join(dff3, Seq("NMPTF"), "left")
      .join(dff4, Seq("NMPTF"), "left")
      .join(dff5, Seq("NMPTF"), "left")
      .join(dff6, Seq("NMPTF"), "left")

    finalDf
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${appGlobalConfig.hdfsExercice02OutputDir}")

    finalDf
  }
}
