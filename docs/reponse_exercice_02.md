# Réponse : Exercice 02 - Comptage des Clients et Contrats par Portefeuille

* Commande de lancement
```shell

./sh/spark-submit.sh --produce exercise02

```
* Le parquet des données produites est disponible ici **"outputs/exercice-02"**


* Faute de frappe sur le nom d’un champ du schema de sortie :
  J'ai remarqué une erreur sur le nom d'une colonne du schema de sortie `ECH_` que j'ai corrigé en `ECH_3`.
  Ce qui nous donne le schema suivant -> `NMPTF`, `TOTAL_CLIENTS`, `TOTAL_CONTRATS`, `ECH_1`, `ECH_2`, `ECH_3`, `ECH4`

* Manque de la notion d'année pour le comptage des contrats avec échéance :
  Bien que nous utilisions la colonne 01255_MOISAN pour compter les contrats échus, il nous faudrait aussi une information sur l'année, pour fiabiliser le calcul. En effet, avec un dataset plus grand, nous n'avons pas de garantie que toutes les données soient sur la même années.
  



