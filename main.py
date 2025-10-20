import requests
import json
import time
import csv
from collections import Counter

class InterpolRedNoticeScraper:
    def __init__(self):
        self.base_url = "https://ws-public.interpol.int/notices/v1/red"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def get_notices(self, page=1, per_page=20):
        """Récupère les notices rouges via l'API publique d'Interpol"""
        params = {
            'page': page,
            'resultPerPage': per_page
        }
        
        try:
            response = requests.get(self.base_url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête: {e}")
            return None
    
    def get_notice_detail(self, entity_id):
        """Récupère les détails complets d'une notice spécifique"""
        url = f"{self.base_url}/{entity_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return None
    
    def extract_notice_info(self, notice):
        """Extrait les informations basiques d'une notice"""
        def safe_join(data):
            if isinstance(data, list):
                return ', '.join(str(x) for x in data)
            elif data:
                return str(data)
            return 'N/A'
        
        return {
            'entity_id': notice.get('entity_id', 'N/A'),
            'name': notice.get('name', 'N/A'),
            'forename': notice.get('forename', 'N/A'),
            'date_of_birth': notice.get('date_of_birth', 'N/A'),
            'nationalities': safe_join(notice.get('nationalities')),
            'place_of_birth': notice.get('place_of_birth', 'N/A'),
            'country_of_birth': notice.get('country_of_birth_id', 'N/A'),
            'sex': notice.get('sex_id', 'N/A'),
            'weight': notice.get('weight', 'N/A'),
            'height': notice.get('height', 'N/A'),
            'eyes_colors': safe_join(notice.get('eyes_colors_id')),
            'hairs_colors': safe_join(notice.get('hairs_id')),
        }
    
    def scrape_all_notices(self, save_to_json=True, save_to_csv=True, get_full_details=True):
        """Scrape TOUTES les notices disponibles"""
        all_notices = []
        page = 1
        
        while True:
            print(f"📄 Récupération de la page {page}...")
            data = self.get_notices(page=page)
            
            if data and '_embedded' in data and 'notices' in data['_embedded']:
                notices = data['_embedded']['notices']
                
                if not notices:  # Plus de notices disponibles
                    print(f"   ✓ Fin des notices atteinte")
                    break
                
                print(f"   ✓ {len(notices)} notices trouvées")
                
                for idx, notice in enumerate(notices, 1):
                    entity_id = notice.get('entity_id', 'N/A')
                    
                    # Récupérer les détails complets si demandé
                    if get_full_details and entity_id != 'N/A':
                        print(f"      [{idx}/{len(notices)}] Détails pour {entity_id}...", end='\r')
                        detailed_notice = self.get_notice_detail(entity_id)
                        if detailed_notice:
                            notice_info = self.extract_notice_info(detailed_notice)
                        else:
                            notice_info = self.extract_notice_info(notice)
                        time.sleep(0.3)  # Pause courte entre chaque requête
                    else:
                        notice_info = self.extract_notice_info(notice)
                    
                    all_notices.append(notice_info)
                
                print(f"   ✓ Page {page} complète (Total: {len(all_notices)})")
                
                # Vérifier s'il y a une page suivante
                total = data.get('total', 0)
                if len(all_notices) >= total:
                    print(f"   ✓ Toutes les notices récupérées ({total} au total)")
                    break
                
                page += 1
                # Pause pour éviter de surcharger le serveur
                time.sleep(1)
            else:
                print(f"   ✗ Aucune notice trouvée sur la page {page}")
                break
        
        print(f"\n✅ Total: {len(all_notices)} notices récupérées")
        
        # Sauvegarde en JSON
        if save_to_json and all_notices:
            with open('interpol_red_notices_ALL.json', 'w', encoding='utf-8') as f:
                json.dump(all_notices, f, ensure_ascii=False, indent=2)
            print("💾 Données sauvegardées dans 'interpol_red_notices_ALL.json'")
        
        # Sauvegarde en CSV
        if save_to_csv and all_notices:
            with open('interpol_red_notices_ALL.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_notices[0].keys())
                writer.writeheader()
                writer.writerows(all_notices)
            print("💾 Données sauvegardées dans 'interpol_red_notices_ALL.csv'")
        
        return all_notices

# Utilisation
if __name__ == "__main__":
    scraper = InterpolRedNoticeScraper()
    
    print("="*60)
    print("🔴 SCRAPER DE NOTICES ROUGES INTERPOL")
    print("="*60)
    print("⚠️  MODE: RÉCUPÉRATION COMPLÈTE AVEC DÉTAILS")
    print("⏱️  Cela va prendre du temps (détails pour chaque notice)...")
    print("="*60 + "\n")
    
    # Scraper TOUTES les notices AVEC détails complets
    notices = scraper.scrape_all_notices(get_full_details=True)
    
    # Statistiques finales
    if notices:
        print("\n" + "="*60)
        print("📊 STATISTIQUES FINALES")
        print("="*60)
        print(f"Total de notices récupérées: {len(notices)}")
        
        # Compter les champs renseignés
        fields_filled = {
            'sex': sum(1 for n in notices if n['sex'] != 'N/A'),
            'weight': sum(1 for n in notices if n['weight'] != 'N/A'),
            'height': sum(1 for n in notices if n['height'] != 'N/A'),
            'eyes': sum(1 for n in notices if n['eyes_colors'] != 'N/A'),
            'hairs': sum(1 for n in notices if n['hairs_colors'] != 'N/A'),
        }
        
        print(f"\nChamps renseignés:")
        print(f"   Sexe: {fields_filled['sex']}/{len(notices)} ({fields_filled['sex']*100//len(notices)}%)")
        print(f"   Poids: {fields_filled['weight']}/{len(notices)} ({fields_filled['weight']*100//len(notices)}%)")
        print(f"   Taille: {fields_filled['height']}/{len(notices)} ({fields_filled['height']*100//len(notices)}%)")
        print(f"   Yeux: {fields_filled['eyes']}/{len(notices)} ({fields_filled['eyes']*100//len(notices)}%)")
        print(f"   Cheveux: {fields_filled['hairs']}/{len(notices)} ({fields_filled['hairs']*100//len(notices)}%)")
        
        # Répartition par sexe
        sexes = [n['sex'] for n in notices if n['sex'] != 'N/A']
        if sexes:
            sex_count = Counter(sexes)
            print(f"\nRépartition par sexe:")
            print(f"   Hommes (M): {sex_count.get('M', 0)}")
            print(f"   Femmes (F): {sex_count.get('F', 0)}")
        
        # Top 10 nationalités
        nationalities = []
        for n in notices:
            if n['nationalities'] != 'N/A':
                nationalities.extend(n['nationalities'].split(', '))
        
        if nationalities:
            top_countries = Counter(nationalities).most_common(10)
            print(f"\n🌍 Top 10 nationalités:")
            for idx, (country, count) in enumerate(top_countries, 1):
                print(f"   {idx}. {country}: {count}")
        
        print("\n--- Exemple de notice ---")
        print(json.dumps(notices[0], ensure_ascii=False, indent=2))
        print("="*60)