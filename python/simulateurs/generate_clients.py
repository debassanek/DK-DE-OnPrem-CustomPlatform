# =================================================================
#  DK-DE-OnPrem-CustomPlatform
#  Script : generate_clients.py
#  Description : Générateur de clients import/export simulés
# =================================================================

import json
import random
from datetime import datetime

# -----------------------------------------------------------------
# Données de référence
# -----------------------------------------------------------------

# Forme juridique

FORME_JURIDIQUES = ["SAS", "SARL", "SA", "EURL", "SCI"]

# Mots pour composer les raisons sociales
PREFIXES_SOCIETE = [
    "TRANSPORTS", "LOGISTIQUE", "IMPORT", "EXPORT", "NEGOCE", "COMMERCE", "DISTRIBUTION", "FRET",
    "MARITIME", "INTERNATIONAL"
]

NOM_SOCIETE = [
    "GLOBAL", "EXPRESS", "SERVICES", "PLUS", "AVENIR", "ELITE", "PRO", "SUD", "NORD", "OUEST", "EST", "EUROPE", "ASIA", "AFRICA", "AMERICA",
    "ATLANTIQUE", "MEDITERRANEE", "PRIME", "OCEANIC", "CONTINENTAL", "MARTIN", "DURAND"
]

# Secteurs d'activité
SECTEURS =["IMPORT", "EXPORT", "IMPORT_EXPORT", "TRANSIT"]

# Villes portuaires françaises
VILLES = [
    "Marseille", "Le Havre", "Rouen", "Dunkerque", "Nantes", "Bordeaux", "La Rochelle", "Saint-Nazaire", "Brest", "Lorient" ,
    "Calais", "Paris", "Dieppe", "Cherbourg", "Saint-Malo", "Roscoff", "Concarneau", "Vannes", "La Ciotat", "Sète", "Toulon", "Nice", "Ajaccio", "Bastia"
]

# -----------------------------------------------------------------
# Fonction génération Siren
# -----------------------------------------------------------------

def generer_siren(index):

    """
    Génère un numéro SIREN unique basé sur l'index.
    Le SIREN est un numéro à 9 chiffres.
    """
    # Assure que l'index est un nombre à 8 chiffres en le complétant avec des zéros si nécessaire
    siren_base = str(index).zfill(8)
    return f"1{siren_base}" # Ajoute un préfixe pour s'assurer qu'il fait 9 chiffres et est unique

# -----------------------------------------------------------------
# Fonction génération Siret
# -----------------------------------------------------------------

def generer_siret(siren):
    """
    Génère un numéro SIRET unique à partir d'un numéro SIREN.
    Le SIRET est composé du SIREN (9 chiffres) et d'un NIC (5 chiffres).
    """
    nic =str(random.randint(0,99999)).zfill(5)
    return f"{siren}{nic}"

# -----------------------------------------------------------------
# Fonction génération client
# -----------------------------------------------------------------

def generer_client(index):
    """Génère un client import/export complet
    """
    
    siren=generer_siren(index)
    
    # Raison sociale : PREFIXE + NOM + FORME
    prefix = random.choice(PREFIXES_SOCIETE)
    nom = random.choice(NOM_SOCIETE)
    forme = random.choice(FORME_JURIDIQUES)
    raison_sociale = f"{prefix} {nom} {forme}"
    
    return {
        "siren"                 :siren,
        "siret"                 :generer_siret(siren),
        "raison_sociale"        :raison_sociale,
        "secteur_activite"      :random.choice(SECTEURS),
        "ville"                 :random.choice(VILLES),
        "code_pays"             :"FR",
        "chiffre_affaires_million":round(random.uniform(0.5, 500), 2),
        "numero_agrement_douane" :f"FR{random.randint(100000,999999)}"
    }
    
# -----------------------------------------------------------------
# Fonction génération dataset (nb_client = 200)
# -----------------------------------------------------------------

def generer_dataset(nb_clients=200):
    """Génère la liste complète des clients
    """
    
    print(f"Génération de {nb_clients} clients")
    
    clients = [generer_client(i) for i in range(nb_clients)]
    
    output = {
        "metadata": {
            "source"                        :"SIMULATED_CLIENTS",
            "date_generation"               :datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "nb_clients"                    :nb_clients
        },
        "clients": clients
    }
    
    chemin = "/tmp/clients_simules.json"
    with open(chemin, 'w',  encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"Fichier généré : {chemin}")
    print(f"Clients : {nb_clients}")
    
    return chemin, output

if __name__ == "__main__":
    chemin, data = generer_dataset(nb_clients=200)
    
    print("\n--- Aperçu des 3 premiers clients ---")
    for c in data["clients"][:3]:
        print(f"""
              {c['raison_sociale']}
              SIREN  : {c['siren']}
              Secteur : {c['secteur_activite']}
              Ville   : {c['ville']}
              CA      : {c['chiffre_affaires_million']} M€ 
            """)
        
        