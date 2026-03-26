import requests
import pandas as pd
import time
import re
import os
import getpass
from collections import Counter

# =============================
# ⚙️ CONFIGURATION
# =============================
BASE = "https://api.eu.cloud.talend.com"

def _load_token():
    """Charge le token depuis la variable d'env TMC_TOKEN, un fichier .env, ou la saisie interactive."""
    # 1. Variable d'environnement
    token = os.environ.get("TMC_TOKEN", "").strip()
    if token and not token.startswith("#"):
        return token

    # 2. Fichier .env dans le répertoire courant
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("TMC_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if token:
                        return token

    # 3. Saisie interactive (masquée)
    print("⚠️  Token TMC non trouvé dans TMC_TOKEN ou .env")
    token = getpass.getpass("🔑 Entre ton token Talend Cloud (Bearer) : ").strip()
    if not token:
        raise SystemExit("❌ Token vide — arrêt du script.")
    return token

TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


# =============================
# 🧰 HTTP UTIL
# =============================
def http_get(url):
    print(f"[GET] {url}")
    resp = requests.get(url, headers=HEADERS)
    if not resp.ok:
        print(f"⚠️ {resp.status_code}: {resp.text}")
        return None
    return resp.json()


# =============================
# 📁 PROJETS
# =============================
def fetch_projects():
    print("\n🔍 Récupération des projets Talend Studio...")
    data = http_get(f"{BASE}/orchestration/projects?limit=100")
    if not data:
        return {}
    projects = data.get("items", []) if isinstance(data, dict) else data
    project_map = {p["id"]: p.get("name") or p.get("technicalLabel") for p in projects if p.get("id")}
    print(f"✅ {len(projects)} projets détectés.")
    return project_map


# =============================
# 🌍 WORKSPACES
# =============================
def fetch_workspaces():
    print("\n🔍 Récupération des workspaces (v2.6 endpoint global)...")
    ws_data = http_get(f"{BASE}/orchestration/workspaces?limit=200")
    if not ws_data:
        return []
    ws_items = ws_data.get("items", []) if isinstance(ws_data, dict) else (ws_data or [])
    workspaces = []
    for w in ws_items:
        env_info = w.get("environment", {})
        workspaces.append({
            "workspace_id": w.get("id"),
            "workspace_name": w.get("name"),
            "environment_id": env_info.get("id"),
            "environment_name": env_info.get("name")
        })
    print(f"✅ {len(workspaces)} workspaces trouvés au total.")
    return workspaces


# =============================
# ⏰ SCHEDULES
# =============================
def fetch_schedules(environment_id):
    all_data, offset = [], 0
    print(f"\n🔄 Récupération des schedules ({environment_id})...")
    while True:
        url = f"{BASE}/orchestration/schedules?limit=100&offset={offset}&environmentId={environment_id}"
        data = http_get(url)
        items = (data or {}).get("items", []) if isinstance(data, dict) else (data or [])
        if not items:
            break
        all_data.extend(items)
        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.2)
    print(f"✅ {len(all_data)} schedules récupérées.")
    return all_data


# =============================
# ⚙️ TASKS
# =============================
def fetch_tasks(environment_id):
    all_data, offset = [], 0
    print(f"\n🔄 Récupération des tasks ({environment_id})...")
    while True:
        url = f"{BASE}/orchestration/executables/tasks?limit=100&offset={offset}&environmentId={environment_id}"
        data = http_get(url)
        items = (data or {}).get("items", []) if isinstance(data, dict) else (data or [])
        if not items:
            break
        all_data.extend(items)
        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.2)
    print(f"✅ {len(all_data)} tasks récupérées.")
    return all_data


# =============================
# 📦 PLANS
# =============================
def fetch_plans(environment_id):
    all_data, offset = [], 0
    print(f"\n🔄 Récupération des plans ({environment_id})...")
    while True:
        url = f"{BASE}/orchestration/executables/plans?limit=100&offset={offset}&environmentId={environment_id}"
        data = http_get(url)
        items = (data or {}).get("items", []) if isinstance(data, dict) else (data or [])
        if not items:
            break
        all_data.extend(items)
        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.2)
    print(f"✅ {len(all_data)} plans récupérés.")
    return all_data


# =============================
# 📦 ARTEFACTS
# =============================
def fetch_artifacts(workspace_id):
    all_data, offset = [], 0
    print(f"\n🔄 Artefacts du workspace {workspace_id}...")
    while True:
        url = f"{BASE}/orchestration/artifacts?limit=100&offset={offset}&workspaceId={workspace_id}"
        data = http_get(url)
        items = (data or {}).get("items", []) if isinstance(data, dict) else (data or [])
        if not items:
            break
        all_data.extend(items)
        if len(items) < 100:
            break
        offset += 100
        time.sleep(0.2)
    print(f"✅ {len(all_data)} artefacts récupérés.")
    return all_data


# =============================
# 🧩 MAPPING WORKSPACE → PROJET
# =============================
def map_workspaces_to_projects(workspaces, project_map):
    workspace_project_map = {}
    print("\n🧩 Correspondance Workspaces ↔ Projets Talend")
    print("--------------------------------------------------")
    for w in workspaces:
        ws_name = (w["workspace_name"] or "").upper()
        env_name = (w["environment_name"] or "")
        matched_project = None
        for pname in project_map.values():
            if pname and ws_name in pname.upper():
                matched_project = pname
                break
        workspace_project_map[w["workspace_id"]] = matched_project or "Inconnu"
        print(f"Workspace: {w['workspace_name']:<12} | Env: {env_name:<6} | Projet: {matched_project or '❌ Aucun'}")
    print("--------------------------------------------------\n")
    return workspace_project_map


# =============================
# 🕒 LECTURE CRON (v3.5 améliorée)
# =============================

def _uniform_step(int_list):
    """Retourne le pas si la liste a un écart constant, sinon None."""
    if len(int_list) < 2:
        return None
    diffs = [b - a for a, b in zip(int_list, int_list[1:])]
    return diffs[0] if all(d == diffs[0] for d in diffs) else None

def _dow_label(dow_raw: str) -> str:
    d = dow_raw.upper().replace(",", ", ")
    if d in ("1-5", "2-6", "MON-FRI"):
        return "jours de semaine"
    if d in ("*", "0-7", "0,1,2,3,4,5,6,7", "0,1,2,3,4,5,6"):
        return "tous les jours"
    return d

def readable_cron(expr: str) -> str:
    if not expr:
        return ""
    expr = expr.strip()

    # ----- Normalisation : champ année wildcard (6 champs → 5 champs) -----
    # ex: "30 4 ? * 2-6 *" → "30 4 ? * 2-6"
    parts = expr.split()
    if len(parts) == 6 and parts[-1] in ('*', '?'):
        expr = ' '.join(parts[:5])

    # ----- Normalisation : dom=* mois=* dow=? → dom=? mois=* dow=* -----
    # ex: "00 20 * * ?"  /  "3,8,13 * * * ?" → formes gérées par les regex suivantes
    parts = expr.split()
    if len(parts) == 5 and parts[2] == '*' and parts[3] == '*' and parts[4] == '?':
        parts[2] = '?'
        parts[4] = '*'
        expr = ' '.join(parts)

    # ----- Cas spéciaux Talend : fin / jour ouvré de mois -----
    if re.search(r"\bL\b", expr) and not re.search(r"\bL[-W]", expr):
        # ex. "5 0 L * ?" → le dernier jour du mois à 00:05
        m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+L\s+\*\s+\?", expr)
        if m:
            minute, hour = map(int, m.groups())
            return f"Le dernier jour du mois à {hour:02d}h{minute:02d}"
        return "Le dernier jour du mois"
    if re.search(r"\bL-1\b", expr):
        # ex. "5 0 L-1 * ?" → le dernier jour du mois à 00:05
        m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+L-1\s+\*\s+\?", expr)
        if m:
            minute, hour = map(int, m.groups())
            return f"Le dernier jour du mois à {hour:02d}h{minute:02d}"
        return "Le dernier jour du mois"
    if re.search(r"\bLW\b", expr):
        # ex. "5 0 LW * ?" → dernier jour ouvré du mois à 00:05
        m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+LW\s+\*\s+\?", expr)
        if m:
            minute, hour = map(int, m.groups())
            return f"Le dernier jour ouvré du mois à {hour:02d}h{minute:02d}"
        return "Le dernier jour ouvré du mois"
    if re.search(r"\b1W\b", expr):
        # ex. "5 0 1W * ?" → 1er jour ouvré du mois à 00:05
        m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+1W\s+\*\s+\?", expr)
        if m:
            minute, hour = map(int, m.groups())
            return f"Le 1er jour ouvré du mois à {hour:02d}h{minute:02d}"
        return "Le 1er jour ouvré du mois"

    # ----- Toutes les X minutes sur une plage horaire (dow optionnel) -----
    # ex: "*/15 7-18 ? * 2,3,4,5,6"  /  "*/17 8-19 ? * 2-6"
    m = re.match(r"^\*/(\d+)\s+(\d{1,2})(?:-(\d{1,2}))?\s+\?\s+\*\s+([0-9A-Z,\-*]+)$", expr)
    if m:
        freq, h1, h2, dow = m.groups()
        h1 = int(h1); h2 = int(h2) if h2 else h1
        lbl = _dow_label(dow)
        if lbl == "jours de semaine":
            return f"Toutes les {int(freq)} min de {h1:02d}h à {h2:02d}h (jours de semaine)"
        if lbl == "tous les jours":
            return f"Toutes les {int(freq)} min de {h1:02d}h à {h2:02d}h"
        return f"Toutes les {int(freq)} min de {h1:02d}h à {h2:02d}h ({lbl})"

    # ----- Liste de minutes + plage d'heures -----
    # ex: "0,30 8-17 ? * 2,3,4,5,6"  → toutes les 30 min 08–17h (jours de semaine)
    m = re.match(r"^([\d,]+)\s+(\d{1,2})-(\d{1,2})\s+\?\s+\*\s+([0-9A-Z,\-*]+)$", expr)
    if m:
        minutes_csv, h1, h2, dow = m.groups()
        mins = [int(x) for x in minutes_csv.split(",") if x.isdigit()]
        step = _uniform_step(sorted(mins))
        lbl = _dow_label(dow)
        if step and set(mins) == set(range(0, 60, step)):
            base = f"Toutes les {step} min"
        elif step:
            base = f"Chaque {step} min (aux minutes {minutes_csv})"
        else:
            base = f"Aux minutes {minutes_csv}"
        suffix = "" if lbl in ("tous les jours",) else f" ({lbl})"
        return f"{base} de {int(h1):02d}h à {int(h2):02d}h{suffix}"

    # ----- Liste de minutes + liste/plage d'heures (générique) -----
    # ex: "0,10,20,30,40,50 5,6,7,8,9,10,... ? * *"
    m = re.match(r"^([\d,]+)\s+([\d,\-]+)\s+\?\s+\*\s+([0-9A-Z,\-*]+)$", expr)
    if m:
        minutes_csv, hours_field, dow = m.groups()
        mins = [int(x) for x in minutes_csv.split(",") if x.isdigit()]
        step = _uniform_step(sorted(mins))
        if "-" in hours_field:
            h1, h2 = [int(x) for x in hours_field.split("-")]
            hours_lbl = f"de {h1:02d}h à {h2:02d}h"
        else:
            hours = [int(h) for h in hours_field.split(",") if h.isdigit()]
            hours_lbl = "à " + ", ".join(f"{h:02d}h" for h in hours)
        lbl = _dow_label(dow)
        if step and set(mins) == set(range(0, 60, step)):
            base = f"Toutes les {step} min"
        elif step:
            base = f"Chaque {step} min (aux minutes {minutes_csv})"
        else:
            base = f"Aux minutes {minutes_csv}"
        suffix = "" if lbl in ("tous les jours",) else f" ({lbl})"
        return f"{base} {hours_lbl}{suffix}"

    # ----- Multi-heures (minute fixe) -----
    # ex: "0 9,14,21 ? * *"  /  "15 8,12,16 ? * 1-5"
    m = re.match(r"^(\d{1,2})\s+([\d,]+)\s+\?\s+\*\s+([0-9A-Z,\-*]+)$", expr)
    if m:
        minute, hours_csv, dow = m.groups()
        hours = [int(h) for h in hours_csv.split(",") if h.isdigit()]
        hour_str = ", ".join(f"{h:02d}h" for h in hours)
        lbl = _dow_label(dow)
        prefix = "Tous les jours" if lbl == "tous les jours" else ("Tous les jours de semaine" if lbl == "jours de semaine" else f"Chaque semaine ({lbl})")
        return f"{prefix} à {hour_str}{'' if int(minute)==0 else f' ({int(minute)}m)'}"

    # ----- Horaire unique (dow/hebdo) -----
    # ex: "20 5 ? * 2-6" / "0 2 ? * *"
    m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+\?\s+\*\s+([0-9A-Z,\-*]+)$", expr)
    if m:
        minute, hour, dow = m.groups()
        lbl = _dow_label(dow)
        if lbl == "jours de semaine":
            return f"Tous les jours de semaine à {int(hour):02d}h{int(minute):02d}"
        if lbl == "tous les jours":
            return f"Tous les jours à {int(hour):02d}h{int(minute):02d}"
        return f"Chaque semaine ({lbl}) à {int(hour):02d}h{int(minute):02d}"

    # ----- Jours du mois + mois (exécutions mensuelles/annuelles) -----
    # ex: "00 8 1-31 1-12 ?" / "10 8 1-31 1-12 ?" / "00 10 29 3,6,9,12 ? *"
    m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+([\d,\-]+)\s+([\d,\-*]+)\s+\?\s*\*?$", expr)
    if m:
        minute, hour, dom, months = m.groups()
        if months in ("*", "1-12"):
            return f"Chaque mois, jours {dom}, à {int(hour):02d}h{int(minute):02d}"
        return f"Jours {dom} des mois {months} à {int(hour):02d}h{int(minute):02d}"

    # ----- Récurrences simples sans plage -----
    # ex: "*/5 * * * * ?" / "*/10 * * * * ?"
    if re.search(r"\*/(\d+)", expr) and "?" in expr:
        freq = int(re.search(r"\*/(\d+)", expr).group(1))
        return f"Toutes les {freq} minutes"

    # ----- Fallback : on remonte l’expression pour debug -----
    return f"(CRON: {expr})"

# =============================
# 🔢 CLASSIFICATION
# =============================
def classify_schedule(desc):
    dl = desc.lower()
    if "5 min" in dl:
        return "Every 5min"
    if "10 min" in dl:
        return "Every 10min"
    if re.search(r"toutes les \d+ min", dl):
        return "Recurring"
    if "aux minutes" in dl or ("chaque" in dl and "min" in dl):
        return "Recurring"
    if "mois" in dl or "mensuel" in dl:
        return "Monthly"
    if "tous les jours" in dl or "quotidien" in dl:
        return "Daily"
    if "semaine" in dl:
        return "Weekly"
    return "Others"


def hour_from_cron(expr):
    try:
        parts = expr.strip().split()
        if len(parts) >= 2:
            h = parts[1]
            m = re.match(r"(\d+)", h)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


# =============================
# 📊 BUILD DATAFRAME
# =============================
def build_dataframe(artifacts, schedules, tasks, plans, workspace_id, workspace_project_map):
    rows = []
    artifact_map = {a["id"]: a for a in artifacts}
    task_map = {t.get("id") or t.get("executable"): t for t in tasks}
    plan_map = {p.get("id") or p.get("executable"): p for p in plans}
    valid_artifacts = {a["id"] for a in artifacts}

    for s in schedules:
        exec_id = s.get("executableId")
        task = task_map.get(exec_id, {})
        plan = plan_map.get(exec_id, {})
        is_plan = bool(plan)
        artifact = artifact_map.get(task.get("artifactId") or "", {})

        if not is_plan and task.get("artifactId") not in valid_artifacts:
            continue

        workspace = (artifact.get("workspace") or {}).get("id", workspace_id)
        project_name = workspace_project_map.get(workspace, "Inconnu")
        environment = ((artifact.get("workspace") or {}).get("environment") or {}).get("name", "")

        for trig in s.get("triggers", []):
            cron_expr = trig.get("cronExpression", "")
            desc = readable_cron(cron_expr)
            sched_type = classify_schedule(desc)
            hour = hour_from_cron(cron_expr)

            pause_info = (task.get("taskPauseDetails") or plan.get("planPauseDetails") or {})
            is_paused = pause_info.get("pause", False)
            if is_paused:
                status = "⏸️ En pause"
            elif cron_expr:
                status = "✅ Planifié"
            else:
                status = "❌ Manuel / Inactif"

            rows.append({
                "Projet": project_name,
                "Workspace": workspace,
                "Type exécutable": "PLAN" if is_plan else "TASK",
                "Nom": plan.get("name", "") if is_plan else task.get("name", ""),
                "Job / Artefact": artifact.get("name", "") if not is_plan else "",
                "Expression CRON": cron_expr,
                "Description": desc,
                "Catégorie": sched_type,
                "Heure": hour,
                "Statut": status,
                "Environnement": environment,
            })
    return pd.DataFrame(rows)


# =============================
# 📈 EXPORT
# =============================
def export_excel(df, project, env):
    safe_project = re.sub(r'[^\w\-]', '_', project)
    safe_env = re.sub(r'[^\w\-]', '_', env)
    output = f"tmc_schedules_{safe_project}_{safe_env}.xlsx"
    print(f"\n💾 Génération du fichier Excel : {output}")

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_sorted = df.sort_values(by=["Type exécutable", "Statut", "Catégorie", "Heure", "Nom"])
        df_sorted.to_excel(writer, sheet_name="Schedules", index=False)

        recap = df["Catégorie"].value_counts().reset_index()
        recap.columns = ["Catégorie", "Nombre"]
        recap.loc[len(recap)] = ["Total", recap["Nombre"].sum()]
        recap.to_excel(writer, sheet_name="Recap", index=False)

        stat = df["Statut"].value_counts().reset_index()
        stat.columns = ["Statut", "Nombre"]
        stat.loc[len(stat)] = ["Total", stat["Nombre"].sum()]
        stat.to_excel(writer, sheet_name="Statuts", index=False)

        afflu = Counter(df.dropna(subset=["Heure"])["Heure"])
        afflu_df = pd.DataFrame(sorted(afflu.items()), columns=["Heure", "Nombre de déclenchements"])
        afflu_df.to_excel(writer, sheet_name="Affluence", index=False)

    print(f"✅ Export terminé : {output}\n")


# =============================
# 🚀 MAIN
# =============================
if __name__ == "__main__":
    print("🚀 Export Talend Cloud Schedules – NextDecision Edition v3.5\n")

    projects = fetch_projects()
    workspaces = fetch_workspaces()
    workspace_project_map = map_workspaces_to_projects(workspaces, projects)

    print("Sélectionne les workspaces à exporter :")
    for i, w in enumerate(workspaces, start=1):
        print(f"[{i}] {w['workspace_name']} – {w['environment_name']}")
    print("[*] Tous les workspaces")

    choice = input("\n👉 Entre les numéros séparés par des virgules (ex: 1,3) ou * pour tout : ").strip()
    if choice == "*":
        selected = workspaces
    else:
        try:
            idx = [int(x.strip()) for x in choice.split(",") if x.strip().isdigit()]
            selected = [workspaces[i - 1] for i in idx if 0 < i <= len(workspaces)]
        except Exception:
            selected = []

    print(f"\n🧭 {len(selected)} workspace(s) sélectionné(s).\n")

    for w in selected:
        project = w["workspace_name"]
        env = w["environment_name"]
        ws_id = w["workspace_id"]
        print(f"🏗️ Traitement du projet {project} – {env}...")

        artifacts = fetch_artifacts(ws_id)
        schedules = fetch_schedules(w["environment_id"])
        tasks = fetch_tasks(w["environment_id"])
        plans = fetch_plans(w["environment_id"])

        df = build_dataframe(artifacts, schedules, tasks, plans, ws_id, workspace_project_map)
        if not df.empty:
            export_excel(df, project, env)
        else:
            print(f"⚠️ Aucun schedule trouvé pour {project} – {env}")
