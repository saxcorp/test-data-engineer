package com.littlebigcode.spark.TestDataEngineer.model

case class AppGlobalConfig(
                            env: String,
                            hdfsRootDataDir: String,
                            hdfsExercice01OutputDir: String,
                            hdfsExercice02OutputDir: String,
                            hdfsExercice03OutputDir: String,
                            hdfsExercice01InputCsvPath: String,
                            hdfsExercice0203InputCampagneParquetPath: String
                          )
