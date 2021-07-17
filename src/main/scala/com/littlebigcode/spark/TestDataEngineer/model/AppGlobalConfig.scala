package com.littlebigcode.spark.TestDataEngineer.model

/**
 * Décrit toutes les variables globales déclarable dans le fichier de configuration de l'application '''application.conf'''.
 *
 * @param env Environement applicatif (DEV/UAT/PRD)
 * @param hdfsRootDataDir Dossier HDFS racine contenant les données produites par l'application.
 * @param hdfsExercice01OutputDir Dossier HDFS racine contenant les données produites par l'exercice 01.
 * @param hdfsExercice02OutputDir Dossier HDFS racine contenant les données produites par l'exercice 02.
 * @param hdfsExercice03OutputDir Dossier HDFS racine contenant les données produites par l'exercice 03.
 * @param hdfsExercice01InputCsvPath Localisation HDFS du fichier csv d'entré pour l'exercice 01.
 * @param hdfsExercice0203InputCampagneParquetPath Localisation HDFS du dossier d'entrée contenant les parquets pour les exercices 02 et 03.
 */
case class AppGlobalConfig(
                            env: String,
                            hdfsRootDataDir: String,
                            hdfsExercice01OutputDir: String,
                            hdfsExercice02OutputDir: String,
                            hdfsExercice03OutputDir: String,
                            hdfsExercice01InputCsvPath: String,
                            hdfsExercice0203InputCampagneParquetPath: String
                          )
