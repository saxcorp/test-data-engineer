package com.littlebigcode.spark.TestDataEngineer.core.exercise

import com.littlebigcode.spark.TestDataEngineer.common.Tools.getStructTypeFromClassPathJson
import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, regexp_replace, to_date, udf}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object Exercise01 {
  val COLUMN_TO_EXCLUDE = Seq("JSON", "TRIM_DOUBLE_QUOTE_JSON", "PARSED_JSON")
  val trimDoubleQuoteFunc: String => String = _.replace("\"{", "{").replace("}\"", "}")
  val trimDoubleQuoteUDF = udf(trimDoubleQuoteFunc)

  val schema = new StructType()
    .add("AGENT_ID1", StringType, nullable = true)
    .add("AGENT_ID2", StringType, nullable = true)
    .add("ADRESSE_PDV2", StringType, nullable = true)
    .add("CLI_TEL", StringType, nullable = true)
    .add("CLI_COEFF", StringType, nullable = true)
    .add("ANCCLI", StringType, nullable = true)
    .add("CDPROENT", StringType, nullable = true)
    .add("CDREGAXA", StringType, nullable = true)
    .add("DATE_LAST_RESIL", StringType, nullable = true)
    .add("DATE_LAST_SOUS", StringType, nullable = true)
    .add("DATE_NAISS", StringType, nullable = true)
    .add("JSON", StringType, nullable = true)

  /**
   *
   * @param spark
   * @param appGlobalConfig
   * @return
   */
  def produceExercise1(spark: SparkSession, appGlobalConfig: AppGlobalConfig) = {
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(schema)
      .csv(appGlobalConfig.hdfsExercice01InputCsvPath)

    val finalDf = df
      .withColumn("AGENT_ID1", col("AGENT_ID1").cast("Long"))
      .withColumn("AGENT_ID2", col("AGENT_ID2").cast("Long"))
      .withColumn("CLI_COEFF", regexp_replace(col("CLI_COEFF"), ",", "."))
      .withColumn("CLI_COEFF", col("CLI_COEFF").cast("Float"))
      .withColumn("ANCCLI", col("ANCCLI").cast("Integer"))
      .withColumn("CDREGAXA", col("CDREGAXA").cast("Integer"))
      .withColumn("DATE_LAST_RESIL", col("DATE_LAST_RESIL").cast(TimestampType))
      .withColumn("DATE_LAST_SOUS", col("DATE_LAST_SOUS").cast(TimestampType))
      .withColumn("DATE_NAISS", to_date(col("DATE_NAISS"), "dd/MM/yyyy"))
      .withColumn("CLI_TEL", regexp_replace(col("CLI_TEL"), "\\D", ""))
      .withColumn("TRIM_DOUBLE_QUOTE_JSON", trimDoubleQuoteUDF(col("JSON")))
      .withColumn("PARSED_JSON", from_json(col("TRIM_DOUBLE_QUOTE_JSON"),
        getStructTypeFromClassPathJson("exercice-01/schema.json")))
      .withColumn("GUID", col("PARSED_JSON.guid"))
      .withColumn("POI", col("PARSED_JSON.poi"))

    val finalSelectedCols = finalDf.schema
      .map(_.name)
      .filter(!COLUMN_TO_EXCLUDE.contains(_))
      .map(col(_))

    finalDf.select(finalSelectedCols: _*)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(appGlobalConfig.hdfsExercice01OutputDir)

    finalDf
  }
}
