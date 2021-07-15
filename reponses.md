# Reponses 

## Description du livrable 
### Caracterisqtique technique 
   * Scala 2.11.12
   * Spark 2.4.8
   * Gradle 6

### Struture de fichier FS et HDFS
* FS (Dossier sur le edge node)
 ```
 [APP_DIR]
  - [lib]
     - lbc-test-data-engineer-1.0.2-SNAPSHOT.jar
  - [conf]
     - application.conf
  - [bin]
     - spark-submit.sh
 ```

* HDFS (Dossier sur le hdfs du cluster)
 ```
 [APP_DIR]
   - [data]
        - [exercise-01]
        - [exercise-02]
        - [exercise-03]
 
 ```

### Installation
Merci de renseigner correctement les variables [template Jinja2](https://jinja.palletsprojects.com/en/3.0.x/templates/#variables) du script sh/spark-submit.sh.
* _application.env_ : Décrit environment (DEV/UAT/PRD)
* _config.spark.kerberos.principal_ : Pour un system securisé via kerberos, indique le principal 
* _config.spark.kerberos.keytab.path_ : Pour un system securisé via kerberos, indique la localisation de la keytab
* _config.spark.driver.memory_ : Décrit le paramettre de configuration de ressource spark "driver.memory"
* _config.spark.executor.memory_ : Décrit le paramettre de configuration de ressource spark "executor.memory"
* _config.spark.executor.cores_ : Décrit le paramettre de configuration de ressource spark "executor.cores"
* _application.config-directory_ : Décrit le paramettre de configuration de ressource spark "config-directory"

Le lancement du test _com.littlebigcode.spark.TestDataEngineer.MainTests_ produira toutes les données sur la base des échantillons.

## [1) Exercice 01 - Conversion d`un CSV vers un DataFrame ](docs/reponse_exercice_01.md)

## [2) Exercice 02 - Comptage des Clients et Contrats par Portefeuille ](docs/reponse_exercice_02.md)

## [3) Exercice 03 - Top 3](docs/reponse_exercice_03.md)

## Conclusion
Le livrable que je viens de produire pour répondre au besoin n'est pas encore complétement ready pour la prod.
Car les problématiques de suivantes n'ont pas été traité car elle nécessite une meilleure connaissance du besoin et des moyens technique mise à disposition par le client :
- Monitoring
- CI/CD
- Rétention de données
- Ordonnancement/Orchestration
- Volume et caractéristique de mise à disposition des données d'entrée
