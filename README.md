# FlightRadar24

# Work done in brief:

- 3 Class: une pour chaque etape ETL
- 3 fichiers pour etl: code/extractors.py, code/transformers.py, code/load.py
- utilisation de parquet (en column) pour permettre un requetage plus rapide
- l'execution du main.py fait apppelle aux 3 scripts ETL.
- un fichier code/queries.py code pour les informations demandés
- les resultats de requetage est enregistré dans le fichier results.txt
- remarque: on limite l'extraction à 11 flights due à des erreurs de "too many calls" de l'API (acces limité aux donnés).
