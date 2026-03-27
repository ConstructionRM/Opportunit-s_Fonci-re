"""
opportunites_foncieres.py
Construction Roger Mercier — Détecteur d'opportunités foncières
Ville de Sherbrooke — Sources : MAMH + ArcGIS Sherbrooke

Usage :
    python opportunites_foncieres.py
    → Génère opportunities.json (à placer à côté du dashboard HTML)

Dépendances :
    pip install requests pandas shapely
"""

import requests
import pandas as pd
import json
import math
import time
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
CONFIG = {
    # ArcGIS — Ville de Sherbrooke (données ouvertes)
    "arcgis_zonage_url":   "https://sigv.ville.sherbrooke.qc.ca/arcgis/rest/services/Donnees_ouvertes/Zonage/MapServer/0/query",
    "arcgis_cadastre_url": "https://sigv.ville.sherbrooke.qc.ca/arcgis/rest/services/Donnees_ouvertes/Cadastre/MapServer/0/query",

    # MAMH — Portail données ouvertes Québec (rôle d'évaluation)
    # Fichier CSV disponible via :
    # https://www.donneesquebec.ca/recherche/dataset/role-evaluation-fonciere
    "mamh_csv_url": "https://www.mamh.gouv.qc.ca/fileadmin/publications/evaluation_fonciere/role_evaluation/role_evaluation_sherbrooke.csv",

    # Paramètres de filtrage
    "superficie_residuelle_min_m2": 380,   # Superficie minimale pour une 2e résidence
    "lot_min_m2_defaut": 400,              # Défaut si non trouvé dans le zonage
    "ratio_terrain_batiment_min": 1.0,     # Terrain doit valoir au moins autant que le bâtiment

    # Zones permettant le multi-logement (à ajuster selon règlement Sherbrooke)
    "zones_multi": ["Rm-1","Rm-2","Rm-3","Rm-4","CM-1","CM-2","CR-1","CR-2","CR-3","CR-4","CH"],

    # Zones résidentielles pour Type 1 (subdivision)
    "zones_residentielles": ["Ra-1","Ra-2","Ra-3","Ra-4","Rb-1","Rb-2"],

    "output_path": "opportunities.json",
    "max_resultats": 500,
}

# ─────────────────────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def safe_get(url: str, params: dict, retries=3) -> dict | None:
    """GET avec retry et timeout."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log(f"  ⚠  Erreur (tentative {attempt+1}/{retries}) : {e}")
            time.sleep(2 ** attempt)
    return None


# ─────────────────────────────────────────────────────────────
# 1. EXTRACTION ZONAGE (ArcGIS)
# ─────────────────────────────────────────────────────────────
def fetch_zonage_sherbrooke() -> pd.DataFrame:
    """
    Requête ArcGIS — couche zonage Sherbrooke.
    Retourne un DataFrame avec : zone, usage_principal, superficie_min_m2,
    largeur_min_m, geometry (centroïd lat/lng approximatif).
    """
    log("Extraction du zonage ArcGIS Sherbrooke…")

    params = {
        "where":         "1=1",
        "outFields":     "DESIGNATION,CATEGORIE,USAGE_PRINCIPAL,SUPERFICIE_MINIMALE,LARGEUR_MINIMALE",
        "f":             "json",
        "resultRecordCount": 1000,
        "returnGeometry": "true",
        "outSR":         "4326",   # WGS84 pour Leaflet
    }

    data = safe_get(CONFIG["arcgis_zonage_url"], params)

    if not data or "features" not in data:
        log("  ✗ ArcGIS inaccessible — génération de données de zonage synthétiques")
        return _zonage_synthetique()

    rows = []
    for f in data["features"]:
        attr = f.get("attributes", {})
        geo  = f.get("geometry", {})
        # Centroïd approximatif à partir du bounding ring
        lat, lng = _extract_centroid(geo)
        rows.append({
            "zone":              attr.get("DESIGNATION", ""),
            "categorie":         attr.get("CATEGORIE", ""),
            "usage_principal":   attr.get("USAGE_PRINCIPAL", ""),
            "superficie_min_m2": attr.get("SUPERFICIE_MINIMALE") or CONFIG["lot_min_m2_defaut"],
            "largeur_min_m":     attr.get("LARGEUR_MINIMALE") or 10,
            "lat":               lat,
            "lng":               lng,
        })

    df = pd.DataFrame(rows)
    log(f"  ✓ {len(df)} zones chargées")
    return df


def _extract_centroid(geo: dict) -> tuple[float, float]:
    """Extrait le centroïd approximatif d'une géométrie ArcGIS (rings)."""
    try:
        rings = geo.get("rings", [])
        if rings:
            pts = rings[0]
            lat = sum(p[1] for p in pts) / len(pts)
            lng = sum(p[0] for p in pts) / len(pts)
            return round(lat, 6), round(lng, 6)
    except Exception:
        pass
    return 45.40, -71.90  # Centre Sherbrooke par défaut


def _zonage_synthetique() -> pd.DataFrame:
    """Données de zonage minimales si ArcGIS est inaccessible."""
    zones = [
        {"zone":"Ra-1","categorie":"résidentiel","usage_principal":"Résidentiel isolé","superficie_min_m2":500,"largeur_min_m":15},
        {"zone":"Ra-2","categorie":"résidentiel","usage_principal":"Résidentiel isolé","superficie_min_m2":420,"largeur_min_m":12},
        {"zone":"Ra-3","categorie":"résidentiel","usage_principal":"Résidentiel semi-détaché","superficie_min_m2":380,"largeur_min_m":10},
        {"zone":"Ra-4","categorie":"résidentiel","usage_principal":"Résidentiel attenant","superficie_min_m2":300,"largeur_min_m":8},
        {"zone":"Rm-2","categorie":"résidentiel_multi","usage_principal":"Résidentiel multi","superficie_min_m2":350,"largeur_min_m":10},
        {"zone":"Rm-3","categorie":"résidentiel_multi","usage_principal":"Multi-logements 3-8 unités","superficie_min_m2":350,"largeur_min_m":10},
        {"zone":"Rm-4","categorie":"résidentiel_multi","usage_principal":"Multi-logements 9+ unités","superficie_min_m2":400,"largeur_min_m":12},
        {"zone":"CM-2","categorie":"commercial_mixte","usage_principal":"Commercial + résidentiel multi","superficie_min_m2":300,"largeur_min_m":8},
        {"zone":"CR-4","categorie":"commercial_résidentiel","usage_principal":"Commercial + résidentiel intensif","superficie_min_m2":200,"largeur_min_m":6},
    ]
    for z in zones:
        z["lat"] = 45.40; z["lng"] = -71.90
    return pd.DataFrame(zones)


# ─────────────────────────────────────────────────────────────
# 2. EXTRACTION RÔLE D'ÉVALUATION (MAMH / CSV)
# ─────────────────────────────────────────────────────────────
def fetch_role_evaluation() -> pd.DataFrame:
    """
    Charge le rôle d'évaluation de Sherbrooke.
    Source primaire : fichier CSV MAMH (données.québec).
    Source de secours : données synthétiques de démonstration.

    Colonnes attendues dans le CSV MAMH :
        MATRICULE, ADRESSE, VALEUR_TOTALE, VALEUR_TERRAIN,
        SUPERFICIE_TERRAIN, LONGITUDE, LATITUDE
    """
    log("Chargement du rôle d'évaluation…")

    # Tentative de chargement du CSV local (si déjà téléchargé)
    local_csv = Path("role_evaluation_sherbrooke.csv")
    if local_csv.exists():
        log(f"  → CSV local trouvé : {local_csv}")
        try:
            df = pd.read_csv(local_csv, encoding="utf-8-sig", low_memory=False)
            df = _normaliser_colonnes_role(df)
            log(f"  ✓ {len(df):,} entrées chargées depuis le CSV local")
            return df
        except Exception as e:
            log(f"  ⚠ Erreur lecture CSV local : {e}")

    # Tentative de téléchargement direct
    log("  → Tentative de téléchargement MAMH…")
    try:
        r = requests.get(CONFIG["mamh_csv_url"], timeout=30, stream=True)
        r.raise_for_status()
        with open(local_csv, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        df = pd.read_csv(local_csv, encoding="utf-8-sig", low_memory=False)
        df = _normaliser_colonnes_role(df)
        log(f"  ✓ {len(df):,} entrées téléchargées")
        return df
    except Exception as e:
        log(f"  ✗ Téléchargement MAMH échoué : {e}")

    log("  → Utilisation de données synthétiques (démo)")
    return _role_synthetique()


def _normaliser_colonnes_role(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les noms de colonnes MAMH vers le schéma interne.
    À adapter selon le format exact du fichier MAMH.
    """
    col_map = {
        # Noms possibles dans les exports MAMH
        "NO_MATRICULE":     "matricule",
        "MATRICULE":        "matricule",
        "ADRESSE_CIVIQUE":  "address",
        "ADRESSE":          "address",
        "VALEUR_TOTALE_IMPOSABLE": "role_total",
        "VALEUR_TOTALE":    "role_total",
        "VALEUR_TERRAIN":   "role_terrain",
        "VALEUR_BATIMENT":  "role_batiment",
        "SUPERFICIE_TERRAIN": "superficie_m2",
        "LONGITUDE":        "lng",
        "LATITUDE":         "lat",
        "NO_ZONE":          "zone",
        "DESIGNATION_ZONE": "zone",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Valeurs dérivées
    if "role_total" in df.columns and "role_terrain" in df.columns:
        df["role_batiment"] = df["role_total"] - df["role_terrain"]
    if "role_batiment" not in df.columns:
        df["role_batiment"] = 0

    # Conversion numérique
    for col in ["role_total","role_terrain","role_batiment","superficie_m2","lat","lng"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filtre Sherbrooke (si données multi-muni)
    if "municipalite" in df.columns:
        df = df[df["municipalite"].str.upper().str.contains("SHERBROOKE", na=False)]

    return df.dropna(subset=["role_total","role_terrain","lat","lng"])


def _role_synthetique() -> pd.DataFrame:
    """Données synthétiques réalistes pour démonstration."""
    import random
    random.seed(42)

    secteurs = {
        "Fleurimont":    (45.402, -71.882),
        "Rock Forest":   (45.372, -71.952),
        "Centre-ville":  (45.403, -71.897),
        "Mont-Bellevue": (45.393, -71.923),
        "Lennoxville":   (45.369, -71.854),
        "Brompton":      (45.416, -71.968),
        "Jacques-Cartier":(45.410, -71.910),
    }
    rues_par_secteur = {
        "Fleurimont":    ["rue Bowen Sud","rue King Est","boul. de l'Université"],
        "Rock Forest":   ["boul. Bourque","rue des Bouleaux","ch. de Saint-Élie"],
        "Centre-ville":  ["rue King Ouest","rue Wellington Nord","rue Dépôt"],
        "Mont-Bellevue": ["rue Galt Ouest","rue Murray","rue Prospect"],
        "Lennoxville":   ["rue de la Rivière","Queen St","College St"],
        "Brompton":      ["rue Gervais","boul. de Brompton","rue Principale"],
        "Jacques-Cartier":["rue Talbot","rue Québec","boul. de Portland"],
    }
    zones_par_secteur = {
        "Fleurimont":    ["Ra-2","Ra-3","Rm-2"],
        "Rock Forest":   ["Ra-1","Ra-2","Ra-3"],
        "Centre-ville":  ["CM-2","CR-4","Rm-4"],
        "Mont-Bellevue": ["Rm-3","Ra-3","CM-2"],
        "Lennoxville":   ["Ra-2","Ra-1","Ra-3"],
        "Brompton":      ["Ra-1","Ra-2","Rm-2"],
        "Jacques-Cartier":["Ra-3","Rm-2","Ra-2"],
    }

    records = []
    for i in range(120):
        secteur = random.choice(list(secteurs.keys()))
        base_lat, base_lng = secteurs[secteur]
        lat = base_lat + random.uniform(-0.015, 0.015)
        lng = base_lng + random.uniform(-0.025, 0.025)
        zone = random.choice(zones_par_secteur[secteur])
        rue  = random.choice(rues_par_secteur[secteur])
        num  = random.randint(100, 3999)

        # Valeurs réalistes Sherbrooke
        role_terrain  = random.randint(60000, 250000)
        role_batiment = random.randint(50000, 350000)
        role_total    = role_terrain + role_batiment
        superficie    = random.randint(420, 2200)

        records.append({
            "matricule":     f"{400 + i//100}-{10 + i%90:02d}-{1000 + i:04d}",
            "address":       f"{num} {rue}",
            "secteur":       secteur,
            "zone":          zone,
            "role_total":    role_total,
            "role_terrain":  role_terrain,
            "role_batiment": role_batiment,
            "superficie_m2": superficie,
            "lat":           round(lat, 6),
            "lng":           round(lng, 6),
        })

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────
# 3. ANALYSE ET SCORING
# ─────────────────────────────────────────────────────────────
def calculer_superficie_residuelle(row: pd.Series, zone_info: dict) -> float | None:
    """
    Estime la superficie de terrain disponible pour une 2e construction.
    Hypothèse : une maison standard occupe ~180 m² d'empreinte + 50 m² autour.
    """
    sup_totale = row.get("superficie_m2", 0)
    if not sup_totale or sup_totale < 200:
        return None
    empreinte_estimee = 230  # m² occupés par le bâtiment existant
    sup_residuelle = sup_totale - empreinte_estimee
    return max(sup_residuelle, 0) if sup_residuelle > 0 else None


def calculer_score(row: pd.Series, type_opp: int) -> int:
    """
    Score d'opportunité de 0 à 100.
    Pondération différente selon le type.
    """
    score = 50  # base

    ratio = row.get("role_terrain", 0) / max(row.get("role_batiment", 1), 1)
    sup   = row.get("superficie_m2", 0)
    res   = row.get("superficie_residuelle_m2") or 0

    if type_opp == 1:  # Subdivision
        # Ratio terrain/bâtiment
        if ratio >= 1.5:  score += 15
        elif ratio >= 1.0: score += 8
        elif ratio < 0.7:  score -= 10

        # Superficie résiduelle
        if res >= 800:   score += 20
        elif res >= 500: score += 12
        elif res >= 380: score += 5
        else:            score -= 15

        # Superficie totale
        if sup >= 1500:  score += 10
        elif sup >= 900: score += 5

    elif type_opp == 2:  # Multi-logement
        # Ratio terrain/bâtiment (plus fort est mieux → terrain valorisé, bâtiment déprecié)
        if ratio >= 2.0:  score += 20
        elif ratio >= 1.5: score += 15
        elif ratio >= 1.0: score += 8
        else:              score -= 10

        # Valeur terrain absolue (signe de localisation)
        val_t = row.get("role_terrain", 0)
        if val_t >= 200000: score += 12
        elif val_t >= 120000: score += 6

        # Zone (densité permise)
        zone = str(row.get("zone",""))
        if zone.startswith("CR"): score += 10
        elif zone.startswith("CM"): score += 7
        elif zone.startswith("Rm"): score += 4

    return max(0, min(100, score))


def generer_signaux(row: pd.Series, type_opp: int, zone_info: dict) -> list[dict]:
    """Génère les signaux qualitatifs pour le drawer de détail."""
    signaux = []
    ratio = row.get("role_terrain",0) / max(row.get("role_batiment",1),1)
    sup_min = zone_info.get("superficie_min_m2", CONFIG["lot_min_m2_defaut"])
    res = row.get("superficie_residuelle_m2") or 0
    zone = str(row.get("zone",""))

    if type_opp == 1:
        signaux.append({
            "ok":   res >= sup_min,
            "text": f"Superficie résiduelle ({res:.0f} m²) {'≥' if res >= sup_min else '<'} minimum réglementaire ({sup_min} m²)"
        })
        signaux.append({
            "ok":   ratio >= 1.0,
            "text": f"Ratio terrain/bâtiment de {ratio:.2f}× — terrain {'bien' if ratio>=1 else 'sous'}-valorisé"
        })
        signaux.append({
            "ok":   True,
            "text": f"Zone {zone} — vérifier si 2e résidence permise au règlement de zonage"
        })
        signaux.append({
            "ok":   False,
            "text": "Largeur de façade et accès au lot arrière à confirmer avec urbanisme"
        })

    elif type_opp == 2:
        signaux.append({
            "ok":   ratio >= 1.0,
            "text": f"Valeur terrain ({ratio*100/(ratio+1):.0f}% de la valeur totale) — {'favorable' if ratio>=1 else 'peu favorable'}"
        })
        nb_log = "3 à 8" if "Rm-3" in zone else "6+" if "CM" in zone else "12+" if "CR" in zone else "multi"
        signaux.append({
            "ok":   True,
            "text": f"Zone {zone} permet logements multiples ({nb_log} unités)"
        })
        if ratio >= 1.5:
            signaux.append({
                "ok":   True,
                "text": "Bâtiment fortement déprécié relativement au terrain — redéveloppement économiquement viable"
            })
        signaux.append({
            "ok":   False,
            "text": "Vérifier historique environnemental (RRTI) avant engagement"
        })

    return signaux


def analyser_opportunites(df_role: pd.DataFrame, df_zonage: pd.DataFrame) -> list[dict]:
    """
    Croise le rôle d'évaluation avec le zonage pour identifier les deux types d'opportunité.
    """
    log("Analyse des opportunités…")

    # Index zonage par zone
    zonage_idx = {}
    for _, z in df_zonage.iterrows():
        zonage_idx[str(z.get("zone",""))] = z.to_dict()

    opportunites = []
    n_t1 = n_t2 = 0

    for _, row in df_role.iterrows():
        zone = str(row.get("zone",""))
        zone_info = zonage_idx.get(zone, {
            "superficie_min_m2": CONFIG["lot_min_m2_defaut"],
            "largeur_min_m": 10,
            "usage_principal": "—"
        })

        role_terrain  = float(row.get("role_terrain", 0) or 0)
        role_batiment = float(row.get("role_batiment", 0) or 0)
        role_total    = float(row.get("role_total", 0) or 0)
        superficie    = float(row.get("superficie_m2", 0) or 0)

        if role_total <= 0 or role_terrain <= 0:
            continue

        ratio = role_terrain / max(role_batiment, 1)
        row = row.copy()
        row["superficie_residuelle_m2"] = calculer_superficie_residuelle(row, zone_info)

        type_opp = None

        # TYPE 1 — Terrain résiduel pour 2e résidence
        if (zone in CONFIG["zones_residentielles"] and
            (row["superficie_residuelle_m2"] or 0) >= CONFIG["superficie_residuelle_min_m2"] and
            ratio >= CONFIG["ratio_terrain_batiment_min"]):
            type_opp = 1
            n_t1 += 1

        # TYPE 2 — Multi-logement sous-évalué
        elif (zone in CONFIG["zones_multi"] and
              ratio >= CONFIG["ratio_terrain_batiment_min"]):
            type_opp = 2
            n_t2 += 1

        if type_opp is None:
            continue

        score = calculer_score(row, type_opp)
        signaux = generer_signaux(row, type_opp, zone_info)

        opportunites.append({
            "id":          len(opportunites) + 1,
            "type":        type_opp,
            "address":     str(row.get("address","—")),
            "secteur":     str(row.get("secteur","Sherbrooke")),
            "lat":         float(row.get("lat", 45.40)),
            "lng":         float(row.get("lng", -71.90)),
            "matricule":   str(row.get("matricule","—")),
            "role_total":  int(role_total),
            "role_terrain":int(role_terrain),
            "role_batiment":int(role_batiment),
            "superficie_m2": int(superficie),
            "superficie_residuelle_m2": int(row["superficie_residuelle_m2"]) if row["superficie_residuelle_m2"] else None,
            "zone":          zone,
            "usage":         zone_info.get("usage_principal","—"),
            "lot_min_m":     int(zone_info.get("largeur_min_m", 10)),
            "lot_min_m2":    int(zone_info.get("superficie_min_m2", CONFIG["lot_min_m2_defaut"])),
            "score":         score,
            "signaux":       signaux,
        })

    # Tri par score décroissant
    opportunites.sort(key=lambda x: x["score"], reverse=True)

    log(f"  ✓ {len(opportunites)} opportunités : {n_t1} Type 1 (subdivision), {n_t2} Type 2 (multi)")
    return opportunites


# ─────────────────────────────────────────────────────────────
# 4. EXPORT JSON
# ─────────────────────────────────────────────────────────────
def exporter_json(data: list[dict], path: str):
    output = {
        "genere_le": datetime.now().isoformat(),
        "municipalite": "Sherbrooke",
        "total": len(data),
        "type_1": sum(1 for d in data if d["type"] == 1),
        "type_2": sum(1 for d in data if d["type"] == 2),
        "opportunites": data
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log(f"  ✓ Exporté → {path} ({Path(path).stat().st_size // 1024} Ko)")


# Note pour le dashboard : il attend un tableau JSON direct (pas d'enveloppe).
# La fonction ci-dessous génère aussi le format attendu par le dashboard HTML.
def exporter_json_dashboard(data: list[dict], path: str):
    """Format simplifié compatible avec le dashboard HTML (tableau direct)."""
    dashboard_path = path.replace(".json", "_dashboard.json")
    with open(dashboard_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # Copier aussi comme opportunities.json (lu directement par le dashboard)
    import shutil
    shutil.copy(dashboard_path, "opportunities.json")
    log(f"  ✓ Dashboard JSON → opportunities.json")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    log("=" * 55)
    log("  Construction Roger Mercier — Opportunités Foncières")
    log("  Sherbrooke, Québec")
    log("=" * 55)

    df_zonage = fetch_zonage_sherbrooke()
    df_role   = fetch_role_evaluation()

    # Ajouter colonne secteur si absente
    if "secteur" not in df_role.columns:
        df_role["secteur"] = "Sherbrooke"

    opportunites = analyser_opportunites(df_role, df_zonage)

    if not opportunites:
        log("⚠  Aucune opportunité détectée — vérifier les paramètres CONFIG.")
        return

    exporter_json(opportunites, CONFIG["output_path"])
    exporter_json_dashboard(opportunites, CONFIG["output_path"])

    log("")
    log("─" * 45)
    log(f"Terminé — {len(opportunites)} opportunités dans opportunities.json")
    log("Copier opportunities.json à côté du fichier HTML.")
    log("─" * 45)


if __name__ == "__main__":
    main()
