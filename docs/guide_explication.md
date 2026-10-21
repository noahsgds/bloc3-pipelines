# Pipeline Sentinel expliqué simplement — guide pour la soutenance (bloc 3)

> À relire avant le jury : ce que c'est, où c'est, ce que je dis, les questions probables.

## 1. En 30 secondes

**Ce que je dis :** « Les serveurs d'Arena Strike envoient un fichier à chaque fin de match.
Toutes les 5 minutes, mon pipeline les récupère, retire les données personnelles inutiles,
vérifie chaque ligne, met les lignes douteuses de côté avec le motif, charge les autres
dans une table propre et les envoie à Sentinel, qui calcule les scores de triche. Chaque
exécution est mesurée, et une alerte part si quelque chose cloche. »

## 2. Airflow — `dags/sentinel_pipeline.py`

**Ce que c'est :** un chef d'orchestre. Il lance les étapes dans l'ordre, toutes les
5 minutes, affiche si chacune a réussi (vert) ou échoué (rouge), et réessaie seul une
étape en échec (3 fois, à 1 minute d'intervalle).

**Questions probables**
- *Pourquoi pas du temps réel avec Kafka ?* Les modérateurs examinent les cas dans la
  journée : 5 minutes de délai ne changent rien. 30 000 fiches par jour, c'est 0,3 par
  seconde : Kafka serait un camion pour transporter une lettre.
- *Pourquoi `max_active_runs=1` ?* Deux exécutions en même temps pourraient lire le même
  fichier. Une seule à la fois, c'est plus simple et plus sûr.

## 3. Les étapes — `python/pipeline/etapes.py`

| Étape | En une phrase |
|---|---|
| Ingestion | Je lis les nouveaux fichiers, je retire pseudo et IP, je copie les lignes en base avec leur origine |
| Contrôles | Je vérifie chaque ligne ; les mauvaises vont en quarantaine avec la raison |
| Transformation | Une requête SQL range les lignes valides dans une table propre, sans doublon |
| Envoi | J'envoie les nouvelles fiches à l'API Sentinel par paquets de 1 000 |
| Surveillance | Je calcule le taux de rejet ; au-delà de 5 %, alerte |
| Purge | Je supprime ce qui a dépassé sa durée de conservation |

**Idée clé : la reprise.** Chaque étape fait « ce qui reste à faire ». Si le pipeline
tombe au milieu, la fois suivante il reprend là où il s'était arrêté, sans rien refaire en
double : un fichier déjà enregistré n'est jamais relu, une fiche déjà envoyée n'est jamais
renvoyée.

**Question probable** — *Que se passe-t-il si l'API Sentinel est en panne ?* L'étape
échoue, Airflow réessaie 3 fois. Les fiches restent marquées « non envoyées » et partent
à la prochaine exécution. Rien n'est perdu (test `test_envoi_reprend_la_ou_il_s_est_arrete`).

## 4. Qualité des données — `python/pipeline/qualite.py`

**Ce que c'est :** pour chaque ligne, une liste de vérifications : champs obligatoires,
identifiants valides, date au bon format, nombres dans des bornes réalistes (une précision
de 170 % est impossible), catégories connues. Le résultat est une liste de motifs en
français ; vide = ligne valide.

**Ce que je dis :** « Une ligne fausse ne bloque jamais le pipeline : elle est mise de côté
avec son motif, et on peut la corriger puis la rejouer. Sur 30 000 lignes simulées, 2 %
sont abîmées : elles sont toutes rattrapées. »

## 5. Surveillance

- Airflow montre l'état et la durée de chaque étape.
- La table `pipeline.mesures` garde, pour chaque étape de chaque exécution : durée, lignes
  traitées, rejets, doublons. La vue `pipeline.executions` en fait une ligne par exécution.
- La table `pipeline.alertes` reçoit les problèmes : fichier illisible, donnée personnelle
  inattendue, trop de rejets, étape en échec.
- Grafana (bloc 2) affiche ces tables en graphiques.

## 6. RGPD

1. **Minimisation** : le pseudo et l'adresse IP arrivent des serveurs, je les supprime avant
   tout stockage.
2. **Violation possible** : si un fichier contient une donnée personnelle imprévue (un
   e-mail), elle est supprimée et une alerte critique demande de prévenir le DPO.
3. **Traçabilité** : chaque fiche sait de quel fichier, de quelle exécution et de quand elle vient.
4. **Droits** : une commande pour l'accès, une pour l'effacement ; chaque demande est enregistrée.
5. **Conservation** : 30 jours pour le brut, 6 mois pour les fiches, purge automatique.
6. **Accès** : un rôle en lecture seule pour la supervision ; mots de passe dans `.env`.

## 7. Tests et CI

25 tests : règles de qualité, pipeline complet, absence de pseudo stocké, fichier illisible,
donnée personnelle inattendue, pas de doublon en relançant, reprise après panne, envoi qui
reprend, alerte de rejet, purge, droits RGPD. La CI GitHub lance ces tests, puis exécute le
DAG pour de vrai dans Airflow et vérifie que des fiches arrivent en base.

## Petit glossaire

| Mot | En simple |
|---|---|
| Pipeline | Chaîne d'étapes qui transforme des données brutes en données utilisables |
| ETL | Extraire, transformer, charger |
| DAG | Le plan des étapes et de leur ordre, dans Airflow |
| Ingestion | Faire entrer les données dans le système |
| Quarantaine | Zone où l'on met de côté les lignes invalides, avec la raison |
| Idempotent | Qu'on peut relancer sans créer de doublon |
| JSON | Format texte de données structurées, envoyé par les serveurs de jeu |
