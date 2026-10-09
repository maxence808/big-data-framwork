# Lancer le Lab 4 — Wikimedia, Kafka et Spark

Ce guide explique comment lancer `notebooks/wikistream_pyspark.ipynb` sous Windows avec PowerShell. Le producteur récupère les modifications Wikimedia, les envoie dans Kafka, puis le notebook les analyse avec Spark Structured Streaming.

## 1. Préparer Docker

Ouvrir **Docker Desktop** et attendre que le moteur soit démarré. Utiliser les conteneurs Linux. Une connexion Internet est nécessaire pour télécharger les images et les dépendances, puis recevoir les événements Wikimedia.

Python, Java et Spark n'ont pas besoin d'être installés sur Windows : ils sont utilisés dans les conteneurs.

Dans PowerShell, se placer dans le dossier du lab :

```powershell
cd C:\Users\maxen\OneDrive\Documents\ING5\big-data-processing\rendu\lab4
docker version
```

`docker version` doit afficher les informations du client et du serveur. Garder ce dossier comme répertoire de travail pour les commandes `docker compose` suivantes.

## 2. Démarrer Kafka et Jupyter

```powershell
docker compose up -d
docker compose ps
```

Le premier démarrage peut prendre plusieurs minutes pour télécharger les images. Les services `kafka` et `pyspark-notebook` doivent être démarrés. Le producteur sera lancé séparément à l'étape 4.

## 3. Ouvrir le notebook

Afficher les logs Jupyter :

```powershell
docker logs pyspark_notebook
```

Copier dans le navigateur le lien contenant `?token=...`. Si l'adresse utilise un nom de conteneur, remplacer uniquement ce nom par `localhost`, en conservant le port `8888`, le chemin et le token.

Dans Jupyter, ouvrir **work → wikistream_pyspark.ipynb**. Attendre l'étape suivante avant d'exécuter ses cellules.

## 4. Alimenter Kafka

Dans le terminal PowerShell, lancer :

```powershell
docker compose run --rm producer
```

Cette commande installe les dépendances du producteur dans son conteneur, crée le topic `wikistreams`, puis envoie les événements pendant **10 minutes**. Par défaut, elle suit les modifications de Wikipédia en français. Laisser ce terminal ouvert pendant la démonstration.

Pour une session de 30 minutes sur plusieurs wikis, utiliser à la place :

```powershell
docker compose run --rm producer --minutes 30 --wiki fr.wikipedia.org en.wikipedia.org
```

Les autres filtres sont décrits dans [README.md](README.md). Conserver le topic par défaut : le notebook lit `wikistreams`.

## 5. Exécuter le notebook

Dans Jupyter, exécuter les cellules dans l'ordre avec **Maj + Entrée**, depuis les imports jusqu'à la lecture des fichiers Parquet.

**Ne pas exécuter la dernière cellule « Stop all the queries » pendant la démonstration : elle arrête les traitements. Éviter « Run All » pour cette raison.**

La création de la session Spark peut prendre du temps au premier lancement, car elle télécharge le connecteur Kafka. Attendre la fin de chaque cellule avant de poursuivre.

Les cellules démarrent plusieurs requêtes en arrière-plan : activité par minute, pages et utilisateurs les plus actifs, tailles des modifications, activité par wiki et alertes pour les modifications d'au moins 5 000 octets.

## 6. Voir les résultats

Ouvrir un **deuxième terminal PowerShell**, puis lancer :

```powershell
docker logs -f pyspark_notebook
```

Les tableaux Spark apparaissent dans ces logs. Leur colonne `query` indique la requête concernée (`q0`, `q1`, etc.). Les résultats des sorties console ne sont pas affichés directement dans les cellules du notebook.

- Attendre au moins 30 à 60 secondes pour les premiers traitements.
- La cellule de monitoring affiche les requêtes actives et les informations du dernier lot ; on peut la réexécuter pour actualiser les informations.
- Les tableaux des pages les plus actives peuvent être vides si aucune page n'a été modifiée au moins deux fois.
- Les alertes peuvent être vides si aucune modification ne dépasse le seuil de 5 000 octets.
- Les alertes sont également écrites dans `data/big_edits`, avec leur checkpoint dans `data/checkpoints/big_edits`. La cellule de lecture Parquet peut être réexécutée après le premier lot, déclenché toutes les minutes.

**Ctrl + C** dans le terminal des logs arrête seulement leur affichage ; les requêtes continuent de fonctionner.

## 7. Arrêter la session

1. Exécuter la dernière cellule du notebook, sous **Stop all the queries**.
2. Si le producteur tourne encore, l'arrêter avec **Ctrl + C** dans son terminal.
3. Enregistrer le notebook dans Jupyter avec **Ctrl + S**.
4. Dans un terminal placé dans `rendu\lab4`, arrêter les services :

```powershell
docker compose down
```

Les fichiers enregistrés dans `notebooks/` et `data/` restent sur le disque. Kafka n'a pas de volume persistant dans cette configuration : ses événements sont perdus lorsque son conteneur est supprimé.

## 8. Relancer et résoudre les problèmes courants

### Docker est inaccessible

Si la commande signale une connexion impossible au moteur Docker, ouvrir Docker Desktop, attendre son démarrage et réessayer `docker version`.

### Jupyter ne s'ouvre pas

Réexécuter `docker logs pyspark_notebook` pour récupérer le lien et le token du lancement actuel. Vérifier que le conteneur est démarré avec `docker compose ps` et que l'adresse utilise `localhost:8888`.

### Le producteur échoue au démarrage

Kafka peut ne pas être prêt immédiatement après `docker compose up -d`. Consulter `docker logs kafka`, attendre son démarrage, puis relancer `docker compose run --rm producer`.

### Aucun résultat n'apparaît

Vérifier que le producteur reçoit des événements, que les cellules de démarrage des requêtes ont été exécutées et que la dernière cellule d'arrêt n'a pas été exécutée. Consulter les logs du notebook pour repérer une erreur Spark. Des tableaux vides peuvent aussi être normaux selon les filtres et les seuils.

### Une requête portant le même nom existe déjà

Avant de relancer les cellules qui démarrent les requêtes, exécuter la dernière cellule pour arrêter les requêtes existantes. Reprendre ensuite les cellules nécessaires. Pour repartir avec une nouvelle session Spark, redémarrer le kernel dans Jupyter, puis exécuter les cellules depuis le début, sauf la dernière.

### La première cellule indique que PySpark manque

Ouvrir le notebook dans Jupyter fourni par Docker, à l'adresse `localhost:8888`. Le Python installé directement sur Windows peut ne pas contenir PySpark.

### Le connecteur Kafka échoue

Le notebook choisit sa version à partir de `pyspark.__version__`. Vérifier les logs : le premier lancement nécessite un accès Internet pour télécharger le connecteur. Si la configuration de la session a été modifiée, redémarrer le kernel avant de réexécuter les cellules.

### Un problème concerne le checkpoint Parquet après une recréation de Kafka

Le checkpoint conservé peut référencer l'ancien topic Kafka. Le notebook utilise `failOnDataLoss=false`, mais cela ne récupère pas les événements perdus. Pour une nouvelle démonstration indépendante, arrêter les requêtes et choisir **deux nouveaux chemins** dans la cellule Parquet : un nouveau dossier de sortie et un nouveau dossier de checkpoint. Cela conserve les résultats précédents.
