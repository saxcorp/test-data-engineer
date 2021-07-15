package com.littlebigcode.spark.TestDataEngineer.core.exercise

import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

object Exercise02 {

  /**
   *
   * @param appGlobalConfig
   * @param dfCampagne
   * @param saveResult
   * @return
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
