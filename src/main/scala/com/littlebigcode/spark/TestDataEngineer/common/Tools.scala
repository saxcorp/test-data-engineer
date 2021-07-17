package com.littlebigcode.spark.TestDataEngineer.common

import com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DataType, StructType}
import scala.io.Source

object Tools {

  /**
   * Construit un '''org.apache.spark.sql.types.StructType''' via un fichier JSON dans le classpath dont la référence est décrite dans l'argument ['''jsonFileClassPath'''].
   *
   * @param jsonFileClassPath Localisation dans le classPath du fichier JSON contenant la definition du schema.
   * @return '''org.apache.spark.sql.types.StructType'''
   */
  def getStructTypeFromClassPathJson(jsonFileClassPath: String) = {
    val url = ClassLoader.getSystemResource(jsonFileClassPath)
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }

  /**
   * Retourne une chaine de caractère présent dans le [path]. [defaultValue] est retourné lorsque le [path] n'existe pas.
   *
   * @param c            [com.typesafe.config]
   * @param path         Localisation de la propriété extraire.
   * @param defaultValue Valeur à retourner lorsque le [path] n'existe pas dans la configuration.
   * @return Valeur contenue dans la propriete identifié par le [path].
   */
  def getConfigStringOrElse(c: Config, path: String, defaultValue: String = ""): String = {
    c.hasPath(path) match {
      case true => c.getString(path)
      case _ => defaultValue
    }
  }

  /**
   * Charge la configuration globale via le fichier application.conf présente dans le classpath.
   *
   * @return [[com.littlebigcode.spark.TestDataEngineer.model.AppGlobalConfig]]
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
