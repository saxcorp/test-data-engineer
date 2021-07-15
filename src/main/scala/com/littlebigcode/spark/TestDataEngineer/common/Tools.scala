package com.littlebigcode.spark.TestDataEngineer.common

import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DataType, StructType}
import scala.io.Source
import scala.util.Try

object Tools {

  /**
   *
   * @param jsonFileClassPath
   * @return
   */
  def getStructTypeFromClassPathJson(jsonFileClassPath: String) = {
    val url = ClassLoader.getSystemResource(jsonFileClassPath)
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }

  /**
   *
   * @param c
   * @param path
   * @param defaultValue
   * @return
   */
  def getConfigStringOrElse(c: Config, path: String, defaultValue: String = ""): String = {
    Try(c.hasPath(path)).isSuccess match {
      case true => c.getString(path)
      case _ => defaultValue
    }
  }

  /**
   *
   * @return
   */
  def getGlobalConfig() = {
    val conf = ConfigFactory.load()
    AppGlobalConfig(
      env = getConfigStringOrElse(conf, "env"),
      hdfsRootDataDir = getConfigStringOrElse(conf, "hdfs.root-data-dir"),
      hdfsExercice01OutputDir = getConfigStringOrElse(conf, "hdfs.exercise-01.output-dir"),
      hdfsExercice02OutputDir = getConfigStringOrElse(conf, "hdfs.exercise-02.output-dir"),
      hdfsExercice03OutputDir = getConfigStringOrElse(conf, "hdfs.exercise-03.output-dir"),
      hdfsExercice01InputCsvPath = getConfigStringOrElse(conf, "hdfs.exercise-01.input.csv-path"),
      hdfsExercice0203InputCampagneParquetPath = getConfigStringOrElse(conf, "hdfs.exercise-02-03.input.campagne-parquet-path")
    )
  }
}
