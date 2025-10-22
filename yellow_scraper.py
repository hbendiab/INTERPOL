#!/usr/bin/env python3
"""
Scraper Interpol Yellow Notices - version complète avec auto-vérification et rattrapage
-------------------------------------------------------------------------------------
✅ Scrape tous les pays (AA-ZZ)
✅ Dépasse la limite de 160 notices via découpage (sexe + âge)
✅ Écrit un CSV complet avec toutes les fiches
✅ Vérifie la complétude (via l'API officielle)
✅ Relance automatiquement les pays incomplets sans filtre
"""

import os, sys, csv, json, time, math, ssl, string, pandas as pd
from itertools import product
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from typing import Dict, Any, List, Optional, Iterable, Set

# ⚠️ CHANGEMENT PRINCIPAL: API URL pour Yellow Notices
API_URL = "https://ws-public.interpol.int/notices/v1/yellow"
RESULTS_PER_PAGE = 160
DELAY = 1.0

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "referer": "https://www.interpol.int/en/How-we-work/Notices/View-Yellow-Notices",  # Modifié pour Yellow
    "accept": "*/*",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ---------- UTILITAIRES HTTP ----------
def http_get_json(url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    if params:
        url = f"{url}?{urlencode(params)}"
    req = Request(url, headers=HEADERS)
    ctx = ssl._create_unverified_context()
    try:
        with urlopen(req, timeout=30, context=ctx) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        return json.loads(data)
    except (HTTPError, URLError) as e:
        print(f"[Erreur HTTP] {e} ({url})")
    except Exception as e:
        print(f"[Erreur inconnue] {e}")
    return {}

def iter_notices(data: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    emb = data.get("_embedded", {})
    arr = emb.get("notices", []) if isinstance(emb, dict) else []
    for item in arr:
        if isinstance(item, dict):
            yield item

# ---------- REQUÊTES FILTRÉES ----------
def fetch_page_with_filters(page: int,
                           nationality: Optional[str] = None,
                           age_min: Optional[int] = None,
                           age_max: Optional[int] = None,
                           sex_id: Optional[str] = None,
                           forename: Optional[str] = None,
                           country_of_birth: Optional[str] = None) -> Dict[str, Any]:
    params = {"page": str(page), "resultPerPage": str(RESULTS_PER_PAGE)}
    if nationality: params["nationality"] = nationality
    if age_min is not None: params["ageMin"] = str(age_min)
    if age_max is not None: params["ageMax"] = str(age_max)
    if sex_id: params["sexId"] = sex_id
    if forename: params["forename"] = forename
    if country_of_birth: params["country_of_birth_id"] = country_of_birth
    return http_get_json(API_URL, params=params)

def get_total_with_filters(nationality=None, age_min=None, age_max=None, sex_id=None, forename=None, country_of_birth=None) -> int:
    data = fetch_page_with_filters(1, nationality, age_min, age_max, sex_id, forename, country_of_birth)
    total = int(data.get("total", 0))
    if total <= 0:
        total = len(list(iter_notices(data)))
    return total

def fetch_all_pages_for_filters(nationality, age_min, age_max, sex_id, seen_ids: Set[str], delay: float, 
                                forename=None, country_of_birth=None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = get_total_with_filters(nationality, age_min, age_max, sex_id, forename, country_of_birth)
    if total == 0:
        return rows
    
    num_pages = math.ceil(total / RESULTS_PER_PAGE)
    
    for page in range(1, num_pages + 1):
        data = fetch_page_with_filters(page, nationality, age_min, age_max, sex_id, forename, country_of_birth)
        for item in iter_notices(data):
            eid = str(item.get("entity_id") or item.get("id") or "").strip()
            nurl = item.get("_links", {}).get("self", {}).get("href", "")
            key = eid or nurl or f"{nationality}|{sex_id}|{age_min}|{page}|{item.get('name','')}"
            
            if key in seen_ids:
                continue
            seen_ids.add(key)
            
            # 🔄 CHAMPS SPÉCIFIQUES AUX YELLOW NOTICES
            rows.append({
                "name": item.get("name", ""),
                "forename": item.get("forename", ""),
                "birth_name": item.get("birth_name", ""),  # Nouveau pour Yellow
                "date_of_birth": item.get("date_of_birth", ""),  # Nouveau pour Yellow
                "place_of_birth": item.get("place_of_birth", ""),  # Nouveau pour Yellow
                "country_of_birth": item.get("country_of_birth_id", ""),  # Nouveau pour Yellow
                "nationality": nationality or "",
                "nationalities": ";".join(item.get("nationalities", [])),  # Nouveau pour Yellow
                "sex": sex_id or item.get("sex_id", ""),
                "height": item.get("height", ""),  # Nouveau pour Yellow
                "weight": item.get("weight", ""),  # Nouveau pour Yellow
                "eyes_colors": ";".join(item.get("eyes_colors_id", [])),  # Nouveau pour Yellow
                "hairs": ";".join(item.get("hairs_id", [])),  # Nouveau pour Yellow
                "distinguishing_marks": item.get("distinguishing_marks", ""),  # Nouveau pour Yellow
                "date_of_event": item.get("date_of_event", ""),  # Nouveau pour Yellow
                "place": item.get("place", ""),  # Nouveau pour Yellow
                "country": item.get("country", ""),  # Nouveau pour Yellow
                "languages": ";".join(item.get("languages_spoken_ids", [])),  # Nouveau pour Yellow
                "father_forename": item.get("father_forename", ""),  # Nouveau pour Yellow
                "mother_forename": item.get("mother_forename", ""),  # Nouveau pour Yellow
                "mother_name": item.get("mother_name", ""),  # Nouveau pour Yellow
                "age_min": age_min if age_min is not None else "",
                "age_max": age_max if age_max is not None else "",
                "entity_id": eid,
                "url": nurl,
                "images_url": item.get("_links", {}).get("images", {}).get("href", ""),  # Nouveau pour Yellow
                "thumbnail_url": item.get("_links", {}).get("thumbnail", {}).get("href", "")  # Nouveau pour Yellow
            })
        
        if page < num_pages:
            time.sleep(delay)
    
    return rows

# ---------- LOGIQUE PAYS → SEXE → ÂGE ----------
def smart_fetch_country(country: str, seen_ids: Set[str], delay: float) -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    
    # Test par nationalité d'abord
    total_country = get_total_with_filters(nationality=country)
    print(f"[Info] {country} (nationalité): total={total_country}")
    
    if total_country <= 160:
        all_rows.extend(fetch_all_pages_for_filters(country, None, None, None, seen_ids, delay))
    else:
        print(f"[Info] {country}: >160, test par genre")
        for sex_id in ["M", "F", "U"]:
            total_sex = get_total_with_filters(country, None, None, sex_id)
            print(f"[Info] {country} sex[{sex_id}]: total={total_sex}")
            
            if total_sex == 0:
                continue
            if total_sex <= 160:
                all_rows.extend(fetch_all_pages_for_filters(country, None, None, sex_id, seen_ids, delay))
            else:
                print(f"[Info] {country} sex[{sex_id}]: >160, utilisation tranches d'âge")
                for age_min in range(0, 120):
                    age_max = age_min + 1
                    total_age = get_total_with_filters(country, age_min, age_max, sex_id)
                    if total_age > 0:
                        all_rows.extend(fetch_all_pages_for_filters(country, age_min, age_max, sex_id, seen_ids, delay))
    
    # 🆕 AUSSI TESTER PAR PAYS DE NAISSANCE pour Yellow Notices
    total_birth_country = get_total_with_filters(country_of_birth=country)
    print(f"[Info] {country} (pays naissance): total={total_birth_country}")
    
    if total_birth_country > 0 and total_birth_country <= 160:
        all_rows.extend(fetch_all_pages_for_filters(None, None, None, None, seen_ids, delay, country_of_birth=country))
    
    return all_rows

# ---------- SCRAPING PRINCIPAL ----------
def run():
    countries = [a + b for a, b in product(string.ascii_uppercase, repeat=2)]
    all_rows: List[Dict[str, str]] = []
    seen_ids: Set[str] = set()
    
    print(f"[Info] Scraping intelligent Yellow Notices pour {len(countries)} pays (~676 combinaisons)")
    
    for i, country in enumerate(countries, 1):
        try:
            rows = smart_fetch_country(country, seen_ids, DELAY)
            all_rows.extend(rows)
            print(f"[OK] {country}: +{len(rows)} notices (total={len(all_rows)})")
        except Exception as e:
            print(f"[Erreur] {country}: {e}")
        
        time.sleep(DELAY * 2)
        
        # Sauvegarde périodique
        if i % 5 == 0:
            fieldnames = [
                "name", "forename", "birth_name", "date_of_birth", "place_of_birth", 
                "country_of_birth", "nationality", "nationalities", "sex", "height", 
                "weight", "eyes_colors", "hairs", "distinguishing_marks", "date_of_event",
                "place", "country", "languages", "father_forename", "mother_forename", 
                "mother_name", "age_min", "age_max", "entity_id", "url", "images_url", "thumbnail_url"
            ]
            
            with open("interpol_yellow_smart_all.csv", "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(all_rows)
            print(f"[Sauvegarde] {len(all_rows)} notices (checkpoint {i})")
    
    # Sauvegarde finale
    fieldnames = [
        "name", "forename", "birth_name", "date_of_birth", "place_of_birth", 
        "country_of_birth", "nationality", "nationalities", "sex", "height", 
        "weight", "eyes_colors", "hairs", "distinguishing_marks", "date_of_event",
        "place", "country", "languages", "father_forename", "mother_forename", 
        "mother_name", "age_min", "age_max", "entity_id", "url", "images_url", "thumbnail_url"
    ]
    
    with open("interpol_yellow_smart_all.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)
    
    print(f"\n✅ [OK] {len(all_rows)} Yellow Notices écrites dans interpol_yellow_smart_all.csv")

# ---------- VÉRIFICATION COMPLÉTUDE ----------
def verify_scraping(input_file="interpol_yellow_smart_all.csv"):
    print("\n[Info] Vérification de la complétude par pays...")
    
    df = pd.read_csv(input_file)
    countries = sorted(df["nationality"].dropna().unique())
    report = []
    
    for country in countries:
        local_count = len(df[df["nationality"] == country])
        data = http_get_json(API_URL, {"page": "1", "resultPerPage": "1", "nationality": country})
        api_total = int(data.get("total", 0))
        missing = api_total - local_count
        percent = 0 if api_total == 0 else round(local_count / api_total * 100, 1)
        
        report.append({
            "country": country,
            "total_api": api_total,
            "local_count": local_count,
            "missing": max(missing, 0),
            "coverage_%": percent
        })
        
        status = "✅ OK" if missing <= 0 else "⚠️ INCOMPLET"
        print(f"[{status}] {country}: {local_count}/{api_total} ({percent}%)")
        time.sleep(0.3)
    
    report_df = pd.DataFrame(report)
    report_df.to_csv("yellow_missing_report.csv", index=False, encoding="utf-8")
    print("\n✅ Rapport sauvegardé dans yellow_missing_report.csv")
    return report_df

# ---------- AUTO-RATTRAPAGE ----------
def auto_rattrapage(input_csv="interpol_yellow_smart_all.csv", threshold=100):
    if not os.path.exists("yellow_missing_report.csv"):
        print("[Erreur] yellow_missing_report.csv introuvable — lance d'abord la vérification.")
        return
    
    df = pd.read_csv("yellow_missing_report.csv")
    missing_countries = df[df["coverage_%"] < threshold]["country"].tolist()
    
    if not missing_countries:
        print("[OK] Tous les pays sont complets.")
        return
    
    print(f"[Auto-rattrapage] {len(missing_countries)} pays à compléter : {', '.join(missing_countries[:10])}...")
    
    seen_ids = set()
    completed_rows = []
    
    for country in missing_countries:
        print(f"\n[Retry] {country} : tentative sans filtre")
        try:
            rows = fetch_all_pages_for_filters(country, None, None, None, seen_ids, delay=1.5)
            print(f"[OK] {country}: {len(rows)} notices récupérées en rattrapage")
            completed_rows.extend(rows)
        except Exception as e:
            print(f"[Erreur rattrapage] {country}: {e}")
    
    if completed_rows:
        print(f"[Sauvegarde] {len(completed_rows)} nouvelles notices récupérées.")
        df_new = pd.DataFrame(completed_rows)
        df_new.to_csv("interpol_yellow_rattrapage.csv", index=False, encoding="utf-8")
        
        df_old = pd.read_csv(input_csv)
        df_all = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["entity_id","url"])
        df_all.to_csv("interpol_yellow_smart_all_corrected.csv", index=False, encoding="utf-8")
        print("\n✅ Fichier fusionné : interpol_yellow_smart_all_corrected.csv")

# ---------- NIVEAU 2 : RATTRAPAGE PAR PAYS DE NAISSANCE ----------
def rattrapage_par_pays_naissance(input_csv="interpol_yellow_smart_all_corrected.csv", threshold=100):
    """
    Niveau 2 : tente de compléter les notices manquantes en recherchant
    par pays de naissance ('country_of_birth_id') spécifique aux Yellow Notices.
    """
    if not os.path.exists("yellow_missing_report.csv"):
        print("[Erreur] yellow_missing_report.csv introuvable — lance d'abord la vérification précédente.")
        return

    df_missing = pd.read_csv("yellow_missing_report.csv")
    df_missing = df_missing[df_missing["coverage_%"] < threshold]
    
    if df_missing.empty:
        print("[OK] Tous les pays sont complets, pas de rattrapage par pays de naissance nécessaire.")
        return

    countries_to_retry = df_missing["country"].tolist()
    print(f"[Niveau 2] Rattrapage par pays de naissance pour {len(countries_to_retry)} pays : {', '.join(countries_to_retry[:10])}...")

    seen_ids = set()
    rows_total: List[Dict[str, str]] = []

    for country in countries_to_retry:
        print(f"\n[Retry/Birth] {country} : tentative via 'country_of_birth_id'")
        try:
            data = http_get_json(API_URL, {"country_of_birth_id": country, "resultPerPage": str(RESULTS_PER_PAGE), "page": "1"})
            total = int(data.get("total", 0))
            print(f"[Info] {country} via pays naissance: total={total}")
            
            if total == 0:
                continue
           
            num_pages = math.ceil(total / RESULTS_PER_PAGE)
            for page in range(1, num_pages + 1):
                data = http_get_json(API_URL, {"country_of_birth_id": country, "resultPerPage": str(RESULTS_PER_PAGE), "page": str(page)})
                notices = list(iter_notices(data))
                
                for item in notices:
                    eid = str(item.get("entity_id") or item.get("id") or "").strip()
                    nurl = item.get("_links", {}).get("self", {}).get("href", "")
                    key = eid or nurl
                    
                    if not key or key in seen_ids:
                        continue
                    seen_ids.add(key)
                    
                    rows_total.append({
                        "name": item.get("name", ""),
                        "forename": item.get("forename", ""),
                        "birth_name": item.get("birth_name", ""),
                        "date_of_birth": item.get("date_of_birth", ""),
                        "place_of_birth": item.get("place_of_birth", ""),
                        "country_of_birth": country,
                        "nationality": "UNK",
                        "entity_id": eid,
                        "url": nurl
                    })
                
                time.sleep(0.5)
            
            print(f"[OK] {country}: +{len(rows_total)} notices ajoutées via pays naissance")
        except Exception as e:
            print(f"[Erreur birth] {country}: {e}")

    if rows_total:
        print(f"\n[Sauvegarde] {len(rows_total)} nouvelles notices via pays de naissance.")
        df_new = pd.DataFrame(rows_total)
        df_new.to_csv("interpol_yellow_rattrapage_birth.csv", index=False, encoding="utf-8")
       
        if os.path.exists(input_csv):
            df_old = pd.read_csv(input_csv)
            df_all = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["entity_id", "url"])
            df_all.to_csv("interpol_yellow_smart_all_final.csv", index=False, encoding="utf-8")
            print("\n✅ Fichier final fusionné : interpol_yellow_smart_all_final.csv")
        else:
            df_new.to_csv("interpol_yellow_smart_all_final.csv", index=False, encoding="utf-8")
            print("\n✅ Fichier final créé : interpol_yellow_smart_all_final.csv")
    else:
        print("[OK] Aucun nouvel enregistrement trouvé via pays de naissance.")

# ---------- MAIN ----------
if __name__ == "__main__":
    print("🟡 Démarrage du scraper Yellow Notices Interpol")
    print("=" * 60)
    
    run()
    verify_scraping()
    auto_rattrapage()
    rattrapage_par_pays_naissance()  # Niveau 2 adapté pour Yellow Notices
    
    print("\n🎯 Scraping Yellow Notices terminé!")
    print("📁 Fichiers générés:")
    print("   - interpol_yellow_smart_all.csv (données principales)")
    print("   - yellow_missing_report.csv (rapport de complétude)")
    print("   - interpol_yellow_smart_all_final.csv (données finales)")