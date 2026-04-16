# Talend Cloud – Export & Analyse des Schedules

## Description

Script Python pour extraire automatiquement l'ensemble des **tâches et plans** de **Talend Cloud (TMC)**, les transformer en un fichier Excel structuré avec graphiques, et analyser la **charge horaire** sur la plateforme.

L'objectif est de faciliter :
- la **visualisation complète** de toutes les tâches (planifiées ou non, standalone ou dans un plan) ;
- la **séparation claire** entre tâches et plans dans des onglets dédiés ;
- la **compréhension des expressions CRON** traduites en texte clair ;
- l'**analyse de charge horaire** pour anticiper les pics d'exécution.

---

## Fonctionnalités principales

- Validation du token API au lancement (arrêt immédiat si invalide)
- Extraction automatique des Schedules, Tasks, Plans et Artefacts via l'API TMC
- Sélection interactive des workspaces à exporter
- Filtrage par workspace (seuls les tâches/plans du workspace sélectionné sont exportés)
- Traduction intelligente des expressions CRON en texte lisible (récurrences, jours ouvrés, derniers jours du mois, etc.)
- Classification automatique des schedules (Every 5min, Every 10min, Recurring, Daily, Weekly, Monthly, Others)
- Séparation des tâches et plans dans des onglets distincts
- Les tâches appartenant à un plan héritent de la planification du plan (colonne "Plan")
- Regroupement des triggers multiples sur une seule ligne par exécutable
- Génération d'un fichier Excel multi-onglets avec tableaux formatés et graphiques :
  - **Tasks** : toutes les tâches avec leur schedule (ou celui du plan), triées par statut et nom
  - **Plans** : tous les plans avec les tâches incluses, triés par statut et nom
  - **Recap** : répartition par catégorie + camembert
  - **Statuts** : répartition par statut + camembert coloré (vert/rouge/orange)
  - **Affluence** : déclenchements par heure (0-23h) + graphe en barres

---

## Prérequis

### Outils nécessaires
- Python **3.10+**
- Accès réseau à l'API **Talend Cloud** (région `eu`)
- Un compte TMC avec les droits :
  - "Orchestration Read"
  - "Processing Read"
  - "Artifact Read"

### Dépendances Python

```bash
pip install requests pandas openpyxl
```

---

## Configuration du token

Le script charge le token API dans cet ordre :

1. **Variable d'environnement** `TMC_TOKEN`
2. **Fichier `.env`** dans le répertoire du script (format `TMC_TOKEN=...`)
3. **Saisie interactive** (masquée) si aucune des deux options précédentes n'est trouvée

---

## Utilisation

```bash
python TMC_schedule.py
```

Le script :
1. Vérifie la validité du token
2. Récupère les projets et workspaces
3. Affiche un menu de sélection des workspaces
4. Pour chaque workspace sélectionné, génère un fichier Excel dans le répertoire du script

Le fichier de sortie est nommé `tmc_schedules_{workspace}_{environnement}.xlsx` avec le chemin complet affiché dans la console.

---

## Structure du fichier Excel

| Onglet | Contenu |
|---|---|
| **Tasks** | Toutes les tâches : Projet, Nom, Artefact, Plan (si applicable), CRON, Description, Catégorie, Statut |
| **Plans** | Tous les plans : Projet, Nom, Tâches incluses, CRON, Description, Catégorie, Statut |
| **Recap** | Nombre d'exécutables par catégorie + camembert |
| **Statuts** | Nombre d'exécutables par statut + camembert coloré |
| **Affluence** | Déclenchements par heure (0-23h) + graphe en barres |
