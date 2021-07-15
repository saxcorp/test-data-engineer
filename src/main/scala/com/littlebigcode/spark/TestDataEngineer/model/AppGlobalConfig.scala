package com.littlebigcode.spark.TestDataEngineer.model

/**
 *
 * @param env
 * @param hdfsRootDataDir
 * @param hdfsExercice01OutputDir
 * @param hdfsExercice02OutputDir
 * @param hdfsExercice03OutputDir
 * @param hdfsExercice01InputCsvPath
 * @param hdfsExercice0203InputCampagneParquetPath
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
