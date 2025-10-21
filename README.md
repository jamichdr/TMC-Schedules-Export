# 🧰 Talend Cloud – Export & Analyse des Schedules

## 📄 Description

Ce script Python permet d’extraire automatiquement l’ensemble des **tâches planifiées (Schedules)** de **Talend Cloud (TMC)**, de les transformer en un fichier Excel structuré et enrichi, et de calculer la **charge horaire estimée** sur la plateforme.

L’objectif est de faciliter :
- la **visualisation complète des Schedules** actifs ;
- la **compréhension des expressions CRON** en texte clair ;
- l’**analyse de charge horaire** pour anticiper les pics d’exécution.

---

## 🚀 Fonctionnalités principales

✅ Extraction automatique des Schedules via l’API TMC  
✅ Association aux Tasks, Plans, et Artefacts correspondants  
✅ Traduction intelligente des CRON en texte lisible  
✅ Calcul d’**affluence horaire** (nombre estimé de flux déclenchés par heure)  
✅ Génération d’un fichier Excel multi-onglets :
- `CRON`, `DAILY`, `WEEKLY`
- `Récapitulatif`
- `Affluence horaire`

---

## 🧱 Prérequis

### 📦 Outils nécessaires
- Python **3.10+**
- Accès réseau à l’API **Talend Cloud** (région `eu`)
- Un compte TMC avec les droits :
  - “Orchestration Read”  
  - “Processing Read”  
  - “Artifact Read”

### 🧰 Dépendances Python
Installer les bibliothèques requises :

```bash
pip install requests pandas openpyxl
