import requests
import pandas as pd
import re
import time
from typing import Dict, Any, List, Optional
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment

# ==========================
# CONFIG
# ==========================
REGION = "eu"
BASE = f"https://api.{REGION}.cloud.talend.com"

API_SCHEDULES = f"{BASE}/orchestration/schedules"
API_TASKS     = f"{BASE}/processing/executables/tasks"
API_PLANS     = f"{BASE}/processing/executables/plans"
API_ARTIFACTS = f"{BASE}/orchestration/artifacts"

API_TOKEN       = "#######"      # <-- Ton token
WORKSPACE_ID    = "#######"
ENVIRONMENT_ID  = "#######"
OUTPUT_FILE     = "tmc_schedules.xlsx"
LIMIT           = 100

# ==========================
# HTTP
# ==========================
def make_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Talend-Workspace-Id": WORKSPACE_ID,
        "X-Talend-Environment-Id": ENVIRONMENT_ID,
    }

def http_get(url: str, params=None) -> Optional[Dict[str, Any]]:
    r = requests.get(url, headers=make_headers(), params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code not in (200, 404):
        print(f"[GET] {url} -> {r.status_code}: {r.text[:200]}")
    return None

# ==========================
# FETCH
# ==========================
def fetch_all_schedules() -> List[Dict[str, Any]]:
    all_items = []
    offset = 0
    print("🔄 Récupération des schedules (en cours)...")
    while True:
        params = {"limit": LIMIT, "offset": offset}
        data = http_get(API_SCHEDULES, params)
        if not data:
            break
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        offset += LIMIT
        if len(items) < LIMIT:
            break
    print(f"✅ {len(all_items)} schedules récupérées.")
    return all_items

def fetch_all_tasks() -> Dict[str, Dict[str, Any]]:
    print("🔄 Récupération des tasks (en cours)...")
    all_items = []
    offset = 0
    while True:
        params = {"limit": LIMIT, "offset": offset}
        data = http_get(API_TASKS, params)
        if not data:
            break
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        offset += LIMIT
        if len(items) < LIMIT:
            break
    print(f"✅ {len(all_items)} tasks récupérées.")
    return {t.get("executable"): t for t in all_items if t.get("executable")}

def fetch_plan_environment(plan_id: str) -> str:
    j = http_get(f"{API_PLANS}/{plan_id}")
    if j and isinstance(j, dict):
        return ((j.get("workspace", {}) or {}).get("environment", {}) or {}).get("name", "") or ""
    return ""

def fetch_artifact_name(artifact_id: str) -> str:
    j = http_get(f"{API_ARTIFACTS}/{artifact_id}")
    if not j:
        return ""
    return j.get("name") or j.get("artifactName") or ""

# ==========================
# CRON → Texte lisible + Charge
# ==========================
DOW_NAMES = {
    "1":"Dim","2":"Lun","3":"Mar","4":"Mer","5":"Jeu","6":"Ven","7":"Sam",
    "SUN":"Dim","MON":"Lun","TUE":"Mar","WED":"Mer","THU":"Jeu","FRI":"Ven","SAT":"Sam"
}

def _list_or_range_to_text(val: str, mapping: dict = None):
    if not val or val == "*" or val == "?":
        return ""
    def mapv(x):
        x = x.strip().upper()
        if mapping and x in mapping: return mapping[x]
        return x
    if "-" in val and "," not in val:
        a,b = val.split("-",1)
        return f"{mapv(a)}–{mapv(b)}"
    parts = [mapv(x) for x in val.split(",") if x.strip()!=""]
    return ", ".join(parts)

def _minutes_per_hour(min_field: str) -> int:
    m = min_field.strip()
    if m == "*": return 60
    if m.startswith("*/"):
        try: step = int(m[2:]); return 60 // step if step > 0 else 0
        except: return 0
    if "," in m: return len([x for x in m.split(",") if x.strip()!=""])
    return 1

def _hour_iter(hour_field: str):
    h = hour_field.strip()
    if h == "*": return range(0,24)
    hours = set()
    for part in h.split(","):
        if "-" in part:
            a,b = part.split("-",1)
            try:
                a,b = int(a), int(b)
                for x in range(a,b+1): hours.add(x)
            except: pass
        else:
            try:
                x = int(part)
                if 0 <= x <= 23: hours.add(x)
            except: pass
    return sorted(hours)

def estimate_hourly_load(trig: Dict[str, Any]) -> Dict[int, int]:
    """Retourne un dict {hour:count}"""
    res = {h:0 for h in range(24)}
    ttype = trig.get("type")
    if ttype == "CRON":
        expr = trig.get("cronExpression","").strip()
        parts = expr.split()
        if len(parts) >= 2:
            mins, hours = parts[0], parts[1]
            per_hour = _minutes_per_hour(mins)
            for h in _hour_iter(hours):
                res[h] += per_hour
    elif ttype in ("DAILY","WEEKLY"):
        times = (trig.get("atTimes") or {}).get("times", [])
        for t in times:
            try: hh = int(t.split(":")[0]); res[hh] += 1
            except: pass
    return res

def cron_to_text(expr: Optional[str]) -> str:
    if not expr: return ""
    c = expr.strip()
    parts = c.split()
    if len(parts) < 4: return f"CRON: {c}"
    mins, hours, dom, mon, dow = parts[0], parts[1], parts[2], parts[3], parts[4] if len(parts)>=5 else "*"

    mins_txt = "toutes les minutes"
    if mins.startswith("*/"):
        mins_txt = f"toutes les {mins[2:]} minutes"
    elif "," in mins:
        mins_txt = f"{len(mins.split(','))} fois par heure"
    elif mins.isdigit():
        mins_txt = f"à {mins.zfill(2)} minutes"

    if "-" in hours:
        a,b = hours.split("-",1)
        hours_txt = f"de {a.zfill(2)}h à {b.zfill(2)}h"
    elif "," in hours:
        hours_txt = "à " + ", ".join(f"{h.zfill(2)}h" for h in hours.split(","))
    elif hours == "*":
        hours_txt = "chaque heure"
    else:
        hours_txt = f"à {hours.zfill(2)}h"

    dow_txt = _list_or_range_to_text(dow, DOW_NAMES)
    if dow_txt: return f"{mins_txt}, {hours_txt}, {dow_txt}"
    return f"{mins_txt}, {hours_txt}"

# ==========================
# BUILD
# ==========================
def build_dataframe() -> pd.DataFrame:
    schedules = fetch_all_schedules()
    tasks_idx = fetch_all_tasks()
    art_cache: Dict[str,str] = {}
    rows = []
    hour_agg = {h:0 for h in range(24)}

    print(f"🔄 Construction du tableau ({len(schedules)} schedules)...")

    for i,s in enumerate(schedules,1):
        if i % 25 == 0: print(f"   → {i}/{len(schedules)} traitées...")
        env_id   = s.get("environmentId")
        exec_id  = s.get("executableId")
        exec_type= s.get("executableType")

        t = tasks_idx.get(exec_id, {})
        task_name = t.get("name","")
        art_id = t.get("artifactId","")
        env_name = fetch_plan_environment(exec_id) if exec_type=="PLAN" else (((t.get("workspace",{}) or {}).get("environment",{}) or {}).get("name",""))

        art_name = ""
        if art_id:
            if art_id not in art_cache:
                art_cache[art_id] = fetch_artifact_name(art_id)
            art_name = art_cache[art_id]

        for trig in s.get("triggers", []):
            ttype = trig.get("type")
            cron_expr = trig.get("cronExpression","")
            desc = cron_to_text(cron_expr) if ttype=="CRON" else f"{ttype} trigger"
            # charge horaire
            load = estimate_hourly_load(trig)
            for h,c in load.items(): hour_agg[h]+=c

            rows.append({
                "Nom artefact": art_name,
                "Nom de la task": task_name,
                "Nom du trigger": trig.get("name"),
                "Type exécutable": exec_type,
                "Type de trigger": ttype,
                "Expression CRON": cron_expr,
                "Description lisible": desc,
                "Heure": trig.get("startTime",""),
                "Date de début": trig.get("startDate"),
                "Fuseau horaire": trig.get("timeZone"),
                "Nom environment": env_name,
                "EnvironmentId": env_id,
                "ID executable": exec_id,
                "ArtifactId": art_id,
            })

    df = pd.DataFrame(rows)
    df.attrs["hour_agg"] = hour_agg
    print("✅ Assemblage terminé.")
    return df

# ==========================
# EXPORT
# ==========================
def export_excel(df: pd.DataFrame, path: str):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet in ["CRON","DAILY","WEEKLY"]:
            sub = df[df["Type de trigger"]==sheet]
            if not sub.empty:
                sub.to_excel(writer, sheet_name=sheet, index=False)

        recap = (
            df.groupby("Type de trigger")
              .size().reset_index(name="Nombre de tâches")
              .sort_values("Type de trigger")
        )
        recap.to_excel(writer, sheet_name="Récapitulatif", index=False)

        hour_agg = df.attrs.get("hour_agg",{h:0 for h in range(24)})
        aff = pd.DataFrame({
            "Heure": [f"{h:02d}h" for h in range(24)],
            "Déclenchements estimés": [hour_agg[h] for h in range(24)]
        })
        aff.to_excel(writer, sheet_name="Affluence horaire", index=False)

    wb = load_workbook(path)
    for ws in wb.worksheets:
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")
        for col in ws.columns:
            w = max(len(str(c.value or "")) for c in col) + 2
            ws.column_dimensions[get_column_letter(col[0].column)].width = w
        ws.auto_filter.ref = ws.dimensions
        ws.freeze_panes = "A2"
    wb.save(path)

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    if "PASTE_YOUR_TOKEN_HERE" in API_TOKEN:
        raise SystemExit("⚠️ Remplace ton token avant d’exécuter.")

    start = time.time()
    df = build_dataframe()
    export_excel(df, OUTPUT_FILE)
    print(f"\n✅ Terminé ! {len(df)} lignes exportées en {round(time.time()-start,1)}s.")
    print(f"📁 Fichier généré : {OUTPUT_FILE}")
